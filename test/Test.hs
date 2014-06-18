module Main where

import Test.Framework (defaultMain)
import qualified Data.RDF.PersistenceTest as PersistenceTest

main :: IO ()
main = defaultMain PersistenceTest.tests
