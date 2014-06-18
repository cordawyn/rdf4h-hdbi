{-# LANGUAGE OverloadedStrings #-}

module Data.RDF.PersistenceTest where

import Data.RDF.Persistence

import Test.Framework.Providers.API
import Test.Framework.Providers.HUnit (testCase)
import qualified Test.HUnit as HU (assert)

import Data.RDF
import Database.HDBI
import Database.HDBI.SQlite
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL

tests :: [Test]
tests = [ testGroup "Persistence tests" allTests ]

allTests :: [Test]
allTests = [ buildTest storeNodeTest ]


-- TESTS

storeNodeTest :: IO Test
storeNodeTest = do
  conn <- connectSqlite3 "test/test.sqlite"
  initStorage conn graph
  storeNode conn graph node
  row <- runFetch conn query () :: IO (Maybe [SqlValue])
  disconnect conn
  return $ testCase "storeNode test" $ assertion row
  where graph = "store_node_test"
        node = unode "http://example.org/rdf/1"
        query = Query $ TL.pack $ "SELECT * FROM " ++ (T.unpack graph) ++ "_nodes"
        expectedResult = [SqlInteger 1, SqlText "UNode", SqlText "http://example.org/rdf/1", SqlNull]
        assertion (Just r) = HU.assert $ expectedResult == r
        assertion Nothing = HU.assert False


-- AUXILIARY

initStorage :: Connection c => c -> GraphName -> IO ()
initStorage conn graph = do
  clearStorage conn graph
  createStorage conn graph

clearStorage :: Connection c => c -> GraphName -> IO ()
clearStorage conn graph = do
  run conn q1 ()
  run conn q2 ()
  where q1 = Query $ TL.pack $ "DROP TABLE IF EXISTS " ++ (T.unpack graph) ++ "_nodes"
        q2 = Query $ TL.pack $ "DROP TABLE IF EXISTS " ++ (T.unpack graph) ++ "_triples"
