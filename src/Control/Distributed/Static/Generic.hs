{-# LANGUAGE CPP #-}

-- | Generic support static labels and static values, as introduced in /Towards
-- Haskell in the Cloud/ (Epstein et al, Haskell Symposium 2011).
--
-- This module provides basic support for /static values/ provided a notion of
-- /static labels/. Labels name values in some arbitrary way. Static values
-- compose labels in arbitrary ways. This module defines interfaces to safely
-- resolve labels to values, on top of which it defines ways to resolve static
-- values and a set of combinators that are generic in the notion of label.
--
-- [Resolving static values]
--
-- A value of type @Static a@ can be resolved to a value of type @a@ through the
-- 'unstatic' function:
--
-- > unstatic :: (Resolve l m st, Typeable a) => st -> Static l a -> m (Either String a)
--
-- 'unstatic' is defined in terms of 'resolve', which maps a static label to
-- a value wrapped with its type:
--
-- > resolve :: st -> l -> m (Either String Dynamic)
--
-- It is a special case of 'unstatic'. 'resolve' is label-specific. 'unstatic'
-- is generic.
--
-- [Dynamic type checking]
--
-- The type representation we use inside 'Dynamic' is not the standard
-- 'Data.Typeable.TypeRep' from "Data.Typeable" but 'Data.Rank1Typeable.TypeRep'
-- from "Data.Rank1Typeable". This means that we can represent polymorphic
-- static values (see below for an example).
--
-- [Compositionality]
--
-- Static values as described in the paper are not compositional: there is no
-- way to combine two static values and get a static value out of it. This makes
-- sense when interpreting static strictly as /known at compile time/, but it
-- severely limits expressiveness. However, the main motivation for 'static' is
-- not that they are known at compile time but rather that /they provide a free/
-- 'Binary' /instance/. We therefore provide two basic constructors for 'Static'
-- values:
--
-- > staticLabel :: l -> Static l a
-- > staticApply :: Static l (a -> b) -> Static l a -> Static l b
--
-- The first constructor refers to a label. The second allows to apply a static
-- function to a static argument, and makes 'Static' compositional: once we have
-- 'staticApply' we can implement numerous derived combinators on 'Static'
-- values (we define a few in this module; see e.g. 'staticCompose',
-- 'staticSplit', 'staticConst', ...).
--
-- [Closures]
--
-- Closures in functional programming arise when we partially apply a function.
-- A closure is a code pointer together with a runtime data structure that
-- represents the value of the free variables of the function. A 'Closure'
-- represents these closures explicitly so that they can be serialized:
--
-- > data Closure a = Closure (Static (ByteString -> a)) ByteString
--
-- See /Towards Haskell in the Cloud/ for the rationale behind representing the
-- function closure environment in serialized ('ByteString') form. Any static
-- value can trivially be turned into a 'Closure' ('staticClosure'). Moreover,
-- since 'Static' is now compositional, we can also define derived operators on
-- 'Closure' values ('closureApplyStatic', 'closureApply', 'closureCompose',
-- 'closureSplit').

{-# OPTIONS_GHC -fno-warn-orphans #-}
module Control.Distributed.Static.Generic
  ( -- * Static values
    Static
  , staticLabel
  , staticApply
  , Resolve(..)
  , unstatic
    -- * Static dictionaries
  , Dict(..)
  , binaryDictEncode
  , binaryDictDecode
  , byteStringBinaryDict
  , pairBinaryDict
    -- * Predefined labels (primitives)
  , PreludeLabel(..)
  , CategoryLabel(..)
  , ArrowLabel(..)
  , ArrowApplyLabel(..)
  , BinaryLabel(..)
    -- * Derived static combinators
  , staticConst
  , staticFlip
  , staticId
  , staticCompose
  , staticFirst
  , staticSecond
  , staticSplit
  , staticFanout
  , staticApp
  , staticEncode
  , staticDecode
  , staticByteStringBinaryDict
  , staticPairBinaryDict
    -- * Closures
  , Closure
  , closure
  , unclosure
    -- * Derived closure combinators
  , staticClosure
  , closureApplyStatic
  , closureApply
  , closureCompose
  , closureSplit
  ) where

import Data.Binary
  ( Binary(get, put)
  , putWord8
  , getWord8
  , encode
  , decode
  )
import Data.ByteString.Lazy (ByteString, empty)
import Control.Applicative ((<$>), (<*>))
import Control.Category (Category(..))
import Control.Monad.Trans.Error (ErrorT(..))
import Data.Rank1Dynamic (Dynamic, fromDynamic, dynApply)
import Data.Typeable (Typeable)
#if !MIN_VERSION_base(4,7,0)
import Data.Typeable (typeOf, mkTyConApp, mkTyCon3)
#endif
import Prelude hiding ((.), id)

--------------------------------------------------------------------------------
-- Static values                                                              --
--------------------------------------------------------------------------------

-- | A static value. 'Static' is opaque; see 'staticLabel' and 'staticApply' for
-- ways to construct static values.
data Static   :: * -> * -> * where
  -- Regular datatype, but use GADT syntax to make types more explicit.
  Static      :: l -> Static l a
  StaticApply :: Static l (a -> b) -> Static l a -> Static l b
  deriving (Typeable)

-- Manual instance necessary due to non Haskell'98 datatype.
instance Show l => Show (Static l a) where
  showsPrec d (Static label)      =
      showParen (d > 10) $ showString "Static " . showsPrec 11 label
  showsPrec d (StaticApply s1 s2) =
      showParen (d > 10) $
        showString "StaticApply " .
        showsPrec 11 s1 .
        showString " " .
        showsPrec 11 s2

instance Binary l => Binary (Static l a) where
  put (Static label) =
      putWord8 0 >> put label
  put (StaticApply s1 s2) =
      putWord8 1 >> put s1 >> put s2
  get = do
      header <- getWord8
      case header of
        0 -> Static <$> get
        1 -> StaticApply <$> get <*> get
        _ -> fail $ "Static.get: invalid."

-- | Instances explain how to resolve static labels.
class Monad m => Resolve l m st | l -> st where
  resolve :: st -> l -> m (Either String Dynamic)

liftEither :: Monad m => Either e a -> ErrorT e m a
liftEither eith = ErrorT $ return $ eith

-- | Resolve a static value.
unstatic :: forall a l m st. (Resolve l m st, Typeable a)
         => st
         -> Static l a
         -> m (Either String a)
unstatic rtable s = runErrorT $ go s >>= liftEither . fromDynamic
  where
    go :: Static l b -> ErrorT String m Dynamic
    go (Static label) = do
        ErrorT $ resolve rtable label
    go (StaticApply s1 s2)  = do
        f <- go s1
        x <- go s2
        liftEither $ f `dynApply` x

-- | Create a primitive static value.
staticLabel :: l -> Static l a
staticLabel = Static

-- | Apply a static value to another.
staticApply :: Static l (a -> b) -> Static l a -> Static l b
staticApply = StaticApply

--------------------------------------------------------------------------------
-- Internal wrappers                                                          --
--------------------------------------------------------------------------------

-- | A reified type class dictionary. Useful for passing dictionaries explicitly.
data Dict c = c => Dict
  deriving (Typeable)

deriving instance Typeable Binary

-- | Version of 'encode' with an explicit dictionary.
binaryDictEncode :: Dict (Binary a) -> a -> ByteString
binaryDictEncode Dict = encode

-- | Version of 'decode' with an explicit dictionary.
binaryDictDecode :: Dict (Binary a) -> ByteString -> a
binaryDictDecode Dict = decode

-- | 'Binary' dictionary for 'ByteString's.
byteStringBinaryDict :: Dict (Binary ByteString)
byteStringBinaryDict = Dict

-- | 'Binary' dictionary for pairs.
pairBinaryDict :: Dict (Binary a) -> Dict (Binary b) -> Dict (Binary (a, b))
pairBinaryDict Dict Dict = Dict

--------------------------------------------------------------------------------
-- Predefined static labels                                                   --
--------------------------------------------------------------------------------

-- | Labels for a few standard 'Prelude' functions.
class PreludeLabel l where
  constLabel   :: l
  flipLabel    :: l

-- | Labels for the methods of the 'Control.Category.Category' class specialized
-- to the '(->)' type constructor.
class CategoryLabel l where
  idLabel      :: l
  composeLabel :: l

-- | Labels for the methods of the 'Control.Arrow.Arrow' class specialized to
-- the '(->)' type constructor.
class CategoryLabel l => ArrowLabel l where
  firstLabel   :: l
  secondLabel  :: l
  splitLabel   :: l
  fanoutLabel  :: l

-- | Labels for the methods of the 'Control.Arrow.ArrowApply' class specialized
-- to the '(->)' type constructor.
class ArrowLabel l => ArrowApplyLabel l where
  appLabel     :: l

-- | Labels for the methods of the 'Data.Binary.Binary' class along with the
-- labels of the dictionaries corresponding to some standard instances.
class BinaryLabel l where
  binaryDictEncodeLabel     :: l
  binaryDictDecodeLabel     :: l
  byteStringBinaryDictLabel :: l
  pairBinaryDictLabel       :: l

--------------------------------------------------------------------------------
-- Combinators on static values                                               --
--------------------------------------------------------------------------------

-- | Static version of 'Prelude.const'.
staticConst :: PreludeLabel l => Static l (a -> b -> a)
staticConst = Static constLabel

-- | Static version of 'Prelude.flip'.
staticFlip :: PreludeLabel l
           => Static l (a -> b -> c)
           -> Static l (b -> a -> c)
staticFlip f = Static flipLabel `staticApply` f

-- | Static version of ('Control.Category.id').
staticId :: (CategoryLabel l) => Static l (a -> a)
staticId = Static idLabel

-- | Static version of ('Control.Category..')
staticCompose :: (CategoryLabel l)
              => Static l (b -> c)
              -> Static l (a -> b)
              -> Static l (a -> c)
staticCompose g f = Static composeLabel `staticApply` g `staticApply` f

-- | Static version of ('Control.Arrow.first').
staticFirst :: (ArrowLabel l)
            => Static l (b -> c) -> Static l ((b, d) -> (c, d))
staticFirst f = Static firstLabel `staticApply` f

-- | Static version of ('Control.Arrow.second').
staticSecond :: (ArrowLabel l)
            => Static l (b -> c) -> Static l ((d, b) -> (d, c))
staticSecond f = Static secondLabel `staticApply` f

-- | Static version of ('Control.Arrow.***').
staticSplit :: (ArrowLabel l)
            => Static l (b -> c)
            -> Static l (b' -> c')
            -> Static l ((b, b') -> (c, c'))
staticSplit f g = Static splitLabel `staticApply` f `staticApply` g

-- | Static version of ('Control.Arrow.&&&').
staticFanout :: (ArrowLabel l)
             => Static l (b -> c)
             -> Static l (b -> c')
             -> Static l (b -> (c, c'))
staticFanout f g = Static fanoutLabel `staticApply` f `staticApply` g

-- | Static version of ('Control.Arrow.app').
staticApp :: (ArrowApplyLabel l)
          => Static l ((b -> c, b) -> c)
staticApp = Static appLabel

-- | Static version of ('encode').
staticEncode :: (BinaryLabel l)
             => Static l (Dict (Binary a))
             -> Static l (a -> ByteString)
staticEncode dict = Static binaryDictEncodeLabel `staticApply` dict

-- | Static version of ('decode').
staticDecode :: (BinaryLabel l)
             => Static l (Dict (Binary a))
             -> Static l (ByteString -> a)
staticDecode dict = Static binaryDictDecodeLabel `staticApply` dict

staticByteStringBinaryDict :: (BinaryLabel l) => Static l (Dict (Binary ByteString))
staticByteStringBinaryDict = Static byteStringBinaryDictLabel

staticPairBinaryDict :: (BinaryLabel l)
                     => Static l (Dict (Binary a))
                     -> Static l (Dict (Binary b))
                     -> Static l (Dict (Binary (a, b)))
staticPairBinaryDict dict1 dict2 =
    Static pairBinaryDictLabel `staticApply` dict1 `staticApply` dict2

--------------------------------------------------------------------------------
-- Closures                                                                   --
--------------------------------------------------------------------------------

-- | A closure is a static value and an encoded environment.
data Closure l a = Closure (Static l (ByteString -> a)) ByteString
  deriving (Typeable, Show)

instance Binary l => Binary (Closure l a) where
  put (Closure static env) = put static >> put env
  get = Closure <$> get <*> get

closure :: Static l (ByteString -> a)           -- ^ Decoder
        -> ByteString                           -- ^ Encoded closure environment
        -> Closure l a
closure = Closure

-- | Resolve a closure.
unclosure :: (Monad m, Resolve l m st, Typeable a)
          => st -> Closure l a -> m (Either String a)
unclosure rtable (Closure static env) = runErrorT $ do
  f <- ErrorT $ unstatic rtable static
  return (f env)

-- | Convert a static value into a closure.
staticClosure :: PreludeLabel l => Static l a -> Closure l a
staticClosure static = closure (staticConst `staticApply` static) empty

--------------------------------------------------------------------------------
-- Combinators on Closures                                                    --
--------------------------------------------------------------------------------

-- | Apply a static function to a closure.
closureApplyStatic :: CategoryLabel l
                   => Static l (a -> b) -> Closure l a -> Closure l b
closureApplyStatic f (Closure decoder env) =
  closure (f `staticCompose` decoder) env

-- | Closure application.
closureApply :: forall a b l.
                (ArrowLabel l, ArrowApplyLabel l, BinaryLabel l, CategoryLabel l)
             => Closure l (a -> b) -> Closure l a -> Closure l b
closureApply (Closure fdec fenv) (Closure xdec xenv) =
    closure decoder (encode (fenv, xenv))
  where
    decoder :: Static l (ByteString -> b)
    decoder =
        staticApp
          `staticCompose`
            (fdec `staticSplit` xdec)
          `staticCompose`
            (staticDecode
               (staticPairBinaryDict
                  staticByteStringBinaryDict
                  staticByteStringBinaryDict))

-- | Closure composition.
closureCompose :: (ArrowLabel l, ArrowApplyLabel l, BinaryLabel l, CategoryLabel l)
               => Closure l (b -> c) -> Closure l (a -> b) -> Closure l (a -> c)
closureCompose g f = Static composeLabel `closureApplyStatic` g `closureApply` f

-- | Closure version of ('Arrow.***').
closureSplit :: (ArrowLabel l, ArrowApplyLabel l, BinaryLabel l, CategoryLabel l)
             => Closure l (b -> c)
             -> Closure l (b' -> c')
             -> Closure l ((b, b') -> (c, c'))
closureSplit f g = Static splitLabel `closureApplyStatic` f `closureApply` g
