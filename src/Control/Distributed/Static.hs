-- | /Towards Haskell in the Cloud/ (Epstein et al, Haskell Symposium 2011)
-- introduces the concept of /static/ values: values that are known at compile
-- time. In a distributed setting where all nodes are running the same
-- executable, static values can be serialized simply by transmitting a code
-- pointer to the value. This however requires special compiler support, which
-- is not yet available in ghc. We can mimick the behaviour by keeping an
-- explicit mapping ('RemoteTable') from labels to values (and making sure that
-- all distributed nodes are using the same 'RemoteTable'). In this module
-- we implement this mimickry and various extensions.
--
-- [Monomorphic example]
--
-- Suppose we are working in the context of some distributed environment, with
-- a monadic type 'Process' representing processes, 'NodeId' representing node
-- addresses and 'ProcessId' representing process addresses. Suppose further
-- that we have a primitive
--
-- > sendInt :: ProcessId -> Int -> Process ()
--
-- We might want to define
--
-- > sendIntClosure :: ProcessId -> Closure (Int -> Process ())
--
-- In order to do that, we need a static version of 'send', and a static
-- decoder for 'ProcessId':
--
-- > sendIntStatic :: Static (ProcessId -> Int -> Process ())
-- > sendIntStatic = staticLabel "$send"
--
-- > decodeProcessIdStatic :: Static (ByteString -> Int)
-- > decodeProcessIdStatic = staticLabel "$decodeProcessId"
--
-- where of course we have to make sure to use an appropriate 'RemoteTable':
--
-- > rtable :: RemoteTable
-- > rtable = registerStatic "$send" (toDynamic sendInt)
-- >        . registerStatic "$decodeProcessId" (toDynamic (decode :: ByteString -> Int))
-- >        $ initRemoteTable
--
-- We can now define 'sendIntClosure':
--
-- > sendIntClosure :: ProcessId -> Closure (Int -> Process ())
-- > sendIntClosure pid = closure decoder (encode pid)
-- >   where
-- >     decoder :: Static (ByteString -> Int -> Process ())
-- >     decoder = sendIntStatic `staticCompose` decodeProcessIdStatic
--
-- [Polymorphic example]
--
-- Suppose we wanted to define a primitive
--
-- > sendIntResult :: ProcessId -> Closure (Process Int) -> Closure (Process ())
--
-- which turns a process that computes an integer into a process that computes
-- the integer and then sends it someplace else.
--
-- We can define
--
-- > bindStatic :: (Typeable a, Typeable b) => Static (Process a -> (a -> Process b) -> Process b)
-- > bindStatic = staticLabel "$bind"
--
-- provided that we register this label:
--
-- > rtable :: RemoteTable
-- > rtable = ...
-- >        . registerStatic "$bind" ((>>=) :: Process ANY1 -> (ANY1 -> Process ANY2) -> Process ANY2)
-- >        $ initRemoteTable
--
-- (Note that we are using the special 'Data.Rank1Typeable.ANY1' and
-- 'Data.Rank1Typeable.ANY2' types from "Data.Rank1Typeable" to represent this
-- polymorphic value.) Once we have a static bind we can define
--
-- > sendIntResult :: ProcessId -> Closure (Process Int) -> Closure (Process ())
-- > sendIntResult pid cl = bindStatic `closureApplyStatic` cl `closureApply` sendIntClosure pid
--
-- [Dealing with qualified types]
--
-- In the above we were careful to avoid qualified types. Suppose that we have
-- instead
--
-- > send :: Binary a => ProcessId -> a -> Process ()
--
-- If we now want to define 'sendClosure', analogous to 'sendIntClosure' above,
-- we somehow need to include the 'Binary' instance in the closure -- after
-- all, we can ship this closure someplace else, where it needs to accept an
-- 'a', /then encode it/, and send it off. In order to do this, we need to turn
-- the Binary instance into an explicit dictionary (in GHC >= 7.8):
--
-- > sendDict :: Dict (Binary a) -> ProcessId -> a -> Process ()
-- > sendDict Dict = send
--
-- Now 'sendDict' is a normal polymorphic value:
--
-- > sendDictStatic :: Static (Dict (Binary a)) -> ProcessId -> a -> Process ())
-- > sendDictStatic = staticLabel "$sendDict"
-- >
-- > rtable :: RemoteTable
-- > rtable = ...
-- >        . registerStatic "$sendDict" (sendDict :: Dict (Binary ANY) -> ProcessId -> ANY -> Process ())
-- >        $ initRemoteTable
--
-- so that we can define
--
-- > sendClosure :: Static (Dict (Binary a)) -> ProcessId -> Closure (a -> Process ())
-- > sendClosure dict pid = closure decoder (encode pid)
-- >   where
-- >     decoder :: Static (ByteString -> a -> Process ())
-- >     decoder = (sendDictStatic `staticApply` dict) `staticCompose` decodeProcessIdStatic
--
-- [Word of Caution]
--
-- You should not /define/ functions on 'ANY' and co. For example, the following
-- definition of 'rtable' is incorrect:
--
-- > rtable :: RemoteTable
-- > rtable = registerStatic "$sdictSendPort" sdictSendPort
-- >        $ initRemoteTable
-- >   where
-- >     sdictSendPort :: Dict (Serializable ANY) -> Dict (Serializable (SendPort ANY))
-- >     sdictSendPort Dict = Dict
--
-- This definition of 'sdictSendPort' ignores its argument completely, and
-- constructs a 'Dict' for the /monomorphic/ type @SendPort ANY@, which isn't
-- what you want. Instead, you should do
--
-- > rtable :: RemoteTable
-- > rtable = registerStatic "$sdictSendPort" (sdictSendPort :: Dict (Serializable ANY)
-- >                                                         -> Dict (Serializable (SendPort ANY)))
-- >        $ initRemoteTable
-- >   where
-- >     sdictSendPort :: forall a. Dict (Serializable a)
-- >                   -> Dict (Serializable (SendPort a))
-- >     sdictSendPort Dict = Dict
module Control.Distributed.Static
  ( -- * Static values
    Static
  , Dict(..)
  , staticLabel
  , staticApply
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
    -- * Derived closure combinators
  , staticClosure
  , closureApplyStatic
  , closureApply
  , closureCompose
  , closureSplit
    -- * Resolution
  , RemoteTable
  , initRemoteTable
  , registerStatic
  , unstatic
  , unclosure
  ) where

import qualified Control.Distributed.Static.Generic as G
import Control.Distributed.Static.Generic (Dict(..))
import Data.Binary
  ( Binary(get, put)
  , encode
  , decode
  )
import Data.ByteString.Lazy (ByteString)
import Data.Map (Map)
import qualified Data.Map as Map (lookup, empty, insert)
import Control.Applicative ((<$>))
import Control.Arrow as Arrow (Arrow(..), ArrowApply(..))
import Data.Functor.Identity (Identity(..))
import Data.Rank1Dynamic (Dynamic, toDynamic)
import Data.Rank1Typeable
  ( Typeable
  , ANY1
  , ANY2
  , ANY3
  , ANY4
  )

--------------------------------------------------------------------------------
-- Introducing static values                                                  --
--------------------------------------------------------------------------------

newtype StaticLabel = StaticLabel String
  deriving (Typeable, Show)

-- | A static value.
type Static = G.Static StaticLabel

instance Binary StaticLabel where
  put (StaticLabel label) = put label
  get = StaticLabel <$> get

-- | Create a primitive static value.
--
-- It is the responsibility of the client code to make sure the corresponding
-- entry in the 'RemoteTable' has the appropriate type.
staticLabel :: String -> Static a
staticLabel = G.staticLabel . StaticLabel

-- | Apply a static value to another.
staticApply :: Static (a -> b) -> Static a -> Static b
staticApply = G.staticApply

--------------------------------------------------------------------------------
-- Eliminating static values                                                  --
--------------------------------------------------------------------------------

-- | Runtime dictionary for 'unstatic' lookups
newtype RemoteTable = RemoteTable (Map String Dynamic)

--------------------------------------------------------------------------------
-- Predefined static labels                                                   --
--------------------------------------------------------------------------------

instance G.PreludeLabel StaticLabel where
  constLabel   = StaticLabel "$const"
  flipLabel    = StaticLabel "$flip"

instance G.CategoryLabel StaticLabel where
  idLabel      = StaticLabel "$id"
  composeLabel = StaticLabel "$compose"

instance G.ArrowLabel StaticLabel where
  firstLabel   = StaticLabel "$first"
  secondLabel  = StaticLabel "$second"
  splitLabel   = StaticLabel "$split"
  fanoutLabel  = StaticLabel "$fanout"

instance G.ArrowApplyLabel StaticLabel where
  appLabel     = StaticLabel "$app"

instance G.BinaryLabel StaticLabel where
  binaryDictEncodeLabel     = StaticLabel "$encode"
  binaryDictDecodeLabel     = StaticLabel "$decode"
  byteStringBinaryDictLabel = StaticLabel "$byteStringDict"
  pairBinaryDictLabel       = StaticLabel "$pairDict"

-- | Initial remote table
initRemoteTable :: RemoteTable
initRemoteTable =
      registerStatic "$const"          (toDynamic (const  :: ANY1 -> ANY2 -> ANY1))
    . registerStatic "$flip"           (toDynamic (flip   :: (ANY1 -> ANY2 -> ANY3)
                                                          -> (ANY2 -> ANY1 -> ANY3)))
    . registerStatic "$id"             (toDynamic (id     :: ANY1 -> ANY1))
    . registerStatic "$compose"        (toDynamic ((.)    :: (ANY2 -> ANY3)
                                                          -> (ANY1 -> ANY2)
                                                          -> (ANY1 -> ANY3)))
    . registerStatic "$first"          (toDynamic (first  :: (ANY1 -> ANY2)
                                                          -> (ANY1, ANY3)
                                                          -> (ANY2, ANY3)))
    . registerStatic "$second"         (toDynamic (second :: (ANY1 -> ANY2)
                                                          -> (ANY3, ANY1)
                                                          -> (ANY3, ANY2)))
    . registerStatic "$split"          (toDynamic ((***)  :: (ANY1 -> ANY3)
                                                          -> (ANY2 -> ANY4)
                                                          -> (ANY1, ANY2)
                                                          -> (ANY3, ANY4)))
    . registerStatic "$fanout"         (toDynamic ((&&&)  :: (ANY1 -> ANY2)
                                                          -> (ANY1 -> ANY3)
                                                          -> (ANY1 -> (ANY2, ANY3))))
    . registerStatic "$app"            (toDynamic (app    :: (ANY1 -> ANY2, ANY1)
                                                          -> ANY2))
    . registerStatic "$encode"         (toDynamic ((\Dict -> encode)
                                                   :: Dict (Binary ANY1)
                                                   -> ANY1
                                                   -> ByteString))
    . registerStatic "$decode"         (toDynamic ((\Dict -> decode)
                                                   :: Dict (Binary ANY1)
                                                   -> ByteString
                                                   -> ANY1))
    . registerStatic "$byteStringDict" (toDynamic (Dict :: Dict (Binary ByteString)))
    . registerStatic "$pairDict"       (toDynamic ((\Dict Dict -> Dict)
                                                   :: Dict (Binary ANY1)
                                                   -> Dict (Binary ANY2)
                                                   -> Dict (Binary (ANY1, ANY2))))
    $ RemoteTable Map.empty

-- | Register a static label
registerStatic :: String -> Dynamic -> RemoteTable -> RemoteTable
registerStatic label dyn (RemoteTable rtable) =
    RemoteTable (Map.insert label dyn rtable)

instance G.Resolve StaticLabel Identity RemoteTable where
  resolve (RemoteTable rtable) (StaticLabel label) =
      case Map.lookup label rtable of
        Nothing -> return $ Left $ "Invalid static label '" ++ label ++ "'"
        Just d  -> return $ Right d

-- | Resolve a static value.
unstatic :: Typeable a => RemoteTable -> Static a -> Either String a
unstatic rtable static = runIdentity $ G.unstatic rtable static

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

staticEncode :: Static (Dict (Binary a))
             -> Static (a -> ByteString)
staticEncode = G.staticEncode

staticDecode :: Static (Dict (Binary a))
             -> Static (ByteString -> a)
staticDecode = G.staticDecode

--------------------------------------------------------------------------------
-- Closures                                                                   --
--------------------------------------------------------------------------------

type Closure = G.Closure StaticLabel

closure :: Static (ByteString -> a) -- ^ Decoder
        -> ByteString               -- ^ Encoded closure environment
        -> Closure a
closure = G.closure

-- | Resolve a closure.
unclosure :: Typeable a => RemoteTable -> Closure a -> Either String a
unclosure rtable clos = runIdentity $ G.unclosure rtable clos

-- | Convert a static value into a closure.
staticClosure :: Typeable a => Static a -> Closure a
staticClosure = G.staticClosure

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
