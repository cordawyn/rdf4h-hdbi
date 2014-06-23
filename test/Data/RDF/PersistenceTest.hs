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
import qualified Data.Sequence as S
import qualified Data.Foldable as F
import qualified Data.Map as Map

tests :: [Test]
tests = [ testGroup "Persistence tests" allTests ]

allTests :: [Test]
allTests = [ buildTest storeNodeTest,
             buildTest storeTripleTest,
             buildTest storeRDFTest ]

testDatabase = "test/test.sqlite"

-- TESTS

storeNodeTest :: IO Test
storeNodeTest = do
  conn <- connectSqlite3 testDatabase
  initStorage conn graph
  storeNode conn graph node
  row <- runFetch conn query ()
  disconnect conn
  return $ testCase "storeNode test" $ assertion row
  where graph = "store_node_test"
        node = unode "http://example.org/rdf/1"
        query = Query $ TL.pack $ "SELECT * FROM " ++ (T.unpack graph) ++ "_nodes"
        expectedResult = [SqlInteger 1, SqlText "UNode", SqlText "http://example.org/rdf/1", SqlNull]
        assertion (Just r) = HU.assert $ expectedResult == r
        assertion Nothing = HU.assert False

storeTripleTest :: IO Test
storeTripleTest = do
  conn <- connectSqlite3 testDatabase
  initStorage conn graph
  storeTriple conn graph trp
  ts <- runFetch conn q1 () :: IO (Maybe [SqlValue])
  case ts of
    Just r -> do
            r2 <- runFetchAll conn q2 $ tail r :: IO (S.Seq [SqlValue])
            disconnect conn
            return $ testCase "storeTriple test" $ assertion $ F.concatMap (\x -> [head x]) r2
    Nothing -> do
            disconnect conn
            return $ testCase "storeTriple test" $ HU.assert False
  where graph = "store_triple_test"
        trp = triple (unode "http://example.org/rdf/2") (unode "http://example.org/rdf/3") (lnode $ plainL "test")
        q1 = Query $ TL.pack $ "SELECT * FROM " ++ (T.unpack graph) ++ "_triples"
        q2 = Query $ TL.pack $ "SELECT * FROM " ++ (T.unpack graph) ++ "_nodes WHERE id IN (?, ?, ?)"
        expectedResult = [SqlInteger 1, SqlInteger 2, SqlInteger 3]
        assertion r = HU.assert $ expectedResult == r

storeRDFTest :: IO Test
storeRDFTest = do
  conn <- connectSqlite3 testDatabase
  initStorage conn graph
  isRDFStored <- storeRDF conn graph rdf
  triplesStored <- runFetchAll conn q1 () :: IO (S.Seq [SqlValue])
  return $ testCase "storeRDF test" $ assertion [isRDFStored, (F.concatMap (\x -> [tail x]) triplesStored) == [t1, t2]]
  where graph = "store_rdf_test"
        rdf = mkRdf trps bUrl pms :: TriplesGraph
        trps = [ triple (unode "http://example.org/rdf/4") (unode "http://example.org/rdf/5") (unode "http://example.org/rdf/6"),
                 triple (unode "http://example.org/rdf/6") (unode "http://example.org/rdf/7") (lnode $ plainLL "test" "en") ]
        bUrl = Just $ BaseUrl "http://example.org/rdf/"
        pms = PrefixMappings Map.empty
        t1 = [SqlInteger 1, SqlInteger 2, SqlInteger 3]
        t2 = [SqlInteger 4, SqlInteger 5, SqlInteger 6]
        q1 = Query $ TL.pack $ "SELECT * FROM " ++ (T.unpack graph) ++ "_triples"
        assertion conds = HU.assert $ all (\x -> x == True) conds

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
