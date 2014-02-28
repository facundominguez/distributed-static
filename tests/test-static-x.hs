{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE StaticValues #-}
{-# LANGUAGE TemplateHaskell #-}

import Control.Distributed.StaticX
import Control.Monad ( when )
import Data.Rank1Typeable ( ANY )
import System.Exit
import Unsafe.Coerce


string :: String
string = "hello"

main :: IO ()
main = do
    let sid0 = $(mkStaticT [| id :: ANY -> ANY |] [t| forall a . a -> a |])
        sid1 = $(mkStaticT [| id . id :: ANY -> ANY |] [t| forall a . a -> a |])
        sarg = $(mkStatic [| string |])

    -- test that resolving the result of staticApply succeeds
    either_s <- unstatic $ sid0 `staticApply` (sid1 `staticApply` sarg)
    when (either_s /= Right string) $
      print either_s >> exitFailure

    -- test that resolving a polymorphic value succeeds
    either_id <- unstatic (sid0 :: Static (ANY -> ANY))
    case either_id of
      Right theId -> let polyId = unsafeCoerce theId :: a -> a
                      in when (polyId string /= string) $
                           print (polyId string) >> exitFailure
      Left err    -> print err >> exitFailure
