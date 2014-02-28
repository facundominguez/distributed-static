{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE StaticValues #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE UnboxedTuples #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

-- | Implementation of Static with the StaticValues extension.
--
module Control.Distributed.StaticX
  ( -- * Static values
    Static
  , Dict(..)
  , staticLabel
  , staticApply
  , mkStatic
  , mkStaticT
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
    -- * Closures
  , Closure
  , closure
  , mkClosure
  , mkClosureN
    -- * Derived closure combinators
  , staticClosure
  , closureApplyStatic
  , closureApply
  , closureCompose
  , closureSplit
    -- * Resolution
  , unstatic
  , unclosure
  ) where

import qualified Control.Distributed.Static.Generic as G
import Control.Distributed.Static.Generic
  ( Dict(..)
  , BinaryDict
  )
import Data.Binary
  ( Binary(get, put)
  , encode
  , decode
  )
import Data.ByteString.Lazy (ByteString)
import Control.Applicative ((<$>), (<*>))
import Control.Arrow as Arrow (Arrow(..), ArrowApply(..))
import Control.Monad ( replicateM )
import Control.Monad.IO.Class ( MonadIO(..) )
import Data.List ( isPrefixOf )
import Data.Rank1Dynamic ( toDynamic )
import Data.Rank1Typeable ( Typeable )

import GHC.Exts         ( addrToAny# )
import GHC.Ptr ( Ptr(..), nullPtr )
import GHC.StaticRef
import Foreign.C.String ( withCString, CString )
import Language.Haskell.TH ( ExpQ, lamE, appE, varE, varP, newName, TypeQ, tupP
                           , tupE
                           )
import Text.Encoding.Z ( zEncodeString )
import System.Info ( os )
import Unsafe.Coerce ( unsafeCoerce )


--------------------------------------------------------------------------------
-- Introducing static values                                                  --
--------------------------------------------------------------------------------

-- | A static value.
type Static = G.Static GlobalName

instance Binary GlobalName where
  put (GlobalName pkg instSfx m n) = put pkg >> put instSfx >> put m >> put n
  get = GlobalName <$> get <*> get <*> get <*> get

-- | Create a primitive static value.
staticLabel :: StaticRef a -> Static a
staticLabel = G.staticLabel . unStaticRef

-- | Apply a static value to another.
staticApply :: Static (a -> b) -> Static a -> Static b
staticApply = G.staticApply

-- | @$(mkStatic [| e :: t |]) :: StaticRef t@ being @Typeable t@.
mkStatic :: ExpQ -> ExpQ
mkStatic e = [| staticLabel $ static
                 (unsafeCoerce (toDynamic $(e)) `asTypeOf` $(e))
              |]

mkStaticT :: ExpQ -> TypeQ -> ExpQ
mkStaticT e t = [| staticLabel $ static
                    (unsafeCoerce $ toDynamic $(e) :: $(t))
                 |]

--------------------------------------------------------------------------------
-- Predefined static labels                                                   --
--------------------------------------------------------------------------------

instance G.PreludeLabel GlobalName where
  constLabel   = unStaticRef $ static const
  flipLabel    = unStaticRef $ static flip

instance G.CategoryLabel GlobalName where
  idLabel      = unStaticRef $ static id
  composeLabel = unStaticRef $ static (.)

instance G.ArrowLabel GlobalName where
  firstLabel   = unStaticRef $ static first
  secondLabel  = unStaticRef $ static second
  splitLabel   = unStaticRef $ static (***)
  fanoutLabel  = unStaticRef $ static (&&&)

instance G.ArrowApplyLabel GlobalName where
  appLabel     = unStaticRef $ static app

instance G.BinaryLabel GlobalName where
  binaryDictEncodeLabel     = unStaticRef $ static encode
  binaryDictDecodeLabel     = unStaticRef $ static decode
  byteStringBinaryDictLabel = unStaticRef $ static (Dict :: BinaryDict ByteString)
  pairBinaryDictLabel       = unStaticRef $ static ((\Dict Dict -> Dict)
                                                   :: BinaryDict a
                                                   -> BinaryDict b
                                                   -> BinaryDict (a,b))

instance MonadIO m => G.Resolve GlobalName m () where
  resolve () gn@(GlobalName pkg _ m n) | "static:" `isPrefixOf` n = do
    let mpkg = case pkg of
                 "main" -> Nothing
                 _ -> Just pkg
    mres <- liftIO $ loadFunction__ mpkg m n
    case mres of
      Nothing -> return $ Left $ "resolve: Unknown static label '" ++ show gn ++ "'"
      Just d  -> return $ Right d

  resolve () gn = return $ Left $ "resolve: Unknown static label '" ++ show gn ++ "'"


loadFunction__ :: Maybe String
              -> String
              -> String
              -> IO (Maybe a)
loadFunction__ mpkg m valsym = do
    let symbol = prefixUnderscore++(maybe "" (\p -> zEncodeString p++"_") mpkg)
                   ++zEncodeString m++"_"++(zEncodeString valsym)++"_closure"
    ptr@(Ptr addr) <- withCString symbol c_lookupSymbol
    if (ptr == nullPtr)
      then return Nothing
      else case addrToAny# addr of
             (# hval #) -> return ( Just hval )
  where
    prefixUnderscore = if elem os ["darwin","mingw32","cygwin"] then "_" else ""

foreign import ccall safe "lookupSymbol"
   c_lookupSymbol :: CString -> IO (Ptr a)

-- | Resolve a static value.
unstatic :: Typeable a => Static a -> IO (Either String a)
unstatic = G.unstatic ()

--------------------------------------------------------------------------------
-- Combinators on static values                                               --
--------------------------------------------------------------------------------

-- | Static version of 'Prelude.const'.
staticConst :: Static (a -> b -> a)
staticConst = G.staticConst

-- | Static version of 'Prelude.flip'.
staticFlip :: Static (a -> b -> c)
           -> Static (b -> a -> c)
staticFlip = G.staticFlip

-- | Static version of ('Control.Category.id').
staticId :: Static (a -> a)
staticId = G.staticId

-- | Static version of ('Control.Category..')
staticCompose :: Static (b -> c)
              -> Static (a -> b)
              -> Static (a -> c)
staticCompose = G.staticCompose

-- | Static version of ('Control.Arrow.first').
staticFirst :: Static (b -> c) -> Static ((b, d) -> (c, d))
staticFirst = G.staticFirst

-- | Static version of ('Control.Arrow.second').
staticSecond :: Static (b -> c) -> Static ((d, b) -> (d, c))
staticSecond = G.staticSecond

-- | Static version of ('Control.Arrow.***').
staticSplit :: Static (b -> c)
            -> Static (b' -> c')
            -> Static ((b, b') -> (c, c'))
staticSplit = G.staticSplit

-- | Static version of ('Control.Arrow.&&&').
staticFanout :: Static (b -> c)
             -> Static (b -> c')
             -> Static (b -> (c, c'))
staticFanout = G.staticFanout

-- | Static version of ('Control.Arrow.app').
staticApp :: Static ((b -> c, b) -> c)
staticApp = G.staticApp

staticEncode :: Static (BinaryDict a)
             -> Static (a -> ByteString)
staticEncode = G.staticEncode

staticDecode :: Static (BinaryDict a)
             -> Static (ByteString -> a)
staticDecode = G.staticDecode

--------------------------------------------------------------------------------
-- Closures                                                                   --
--------------------------------------------------------------------------------

type Closure = G.Closure GlobalName

closure :: Static (ByteString -> a) -- ^ Decoder
        -> ByteString               -- ^ Encoded closure environment
        -> Closure a
closure = G.closure

-- | Resolve a closure.
unclosure :: Typeable a => Closure a -> IO (Either String a)
unclosure = G.unclosure ()

-- | Convert a static value into a closure.
staticClosure :: Typeable a => Static a -> Closure a
staticClosure = G.staticClosure

-- | @unclosure ($(mkClosure n [| e |]) x1 ... xn) == e x1 ... xn@
--
-- > $(mkClosureN n e) = G.closure (\(x1,...,xn) -> e x1 ... xn) . decode) (encode (x1,...,xn))
--
mkClosureN :: Int -> ExpQ -> ExpQ
mkClosureN n e = do
    ns <- replicateM n $ newName "x"
    let vs = map varE ns
        ps = map varP ns
    lamE ps [| G.closure $(mkStatic [| $(lamE [tupP ps] (foldl appE e vs)) . decode |])
                         (encode $(tupE vs))
            |]

-- | @mkClosure = mkClosureN 1@
mkClosure :: ExpQ -> ExpQ
mkClosure = mkClosureN 1

--------------------------------------------------------------------------------
-- Combinators on Closures                                                    --
--------------------------------------------------------------------------------

-- | Apply a static function to a closure.
closureApplyStatic :: Static (a -> b) -> Closure a -> Closure b
closureApplyStatic = G.closureApplyStatic

-- | Closure application.
closureApply :: Closure (a -> b) -> Closure a -> Closure b
closureApply = G.closureApply

-- | Closure composition.
closureCompose :: Closure (b -> c) -> Closure (a -> b) -> Closure (a -> c)
closureCompose = G.closureCompose

-- | Closure version of ('Arrow.***').
closureSplit :: Closure (b -> c)
             -> Closure (b' -> c')
             -> Closure ((b, b') -> (c, c'))
closureSplit = G.closureSplit
