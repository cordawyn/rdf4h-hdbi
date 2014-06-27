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
  disconnect conn
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

loadNodeTest :: IO Test
loadNodeTest = do
  conn <- connectSqlite3 testDatabase
  initStorage conn graph
  run conn query ()
  nid <- lastInsertedRowId conn -- TODO: "lastInsertedRowId" is not supposed to be available from Data.RDF.Persistence, but from "hdbi-*" modules.
  n <- loadNode conn graph nid
  disconnect conn
  case n of
    Nothing -> return $ testCase "loadNode test" $ HU.assert False
    Just node -> return $ testCase "loadNode test" $ HU.assert $ node == unode "http://example.org/rdf/1"
  where graph = "load_node_test"
        query = Query $ TL.pack $ "INSERT INTO `" ++ (T.unpack graph) ++ "_nodes` (type, text) VALUES ('UNode', 'http://example.org/rdf/1')"

loadTripleTest :: IO Test
loadTripleTest = do
  conn <- connectSqlite3 testDatabase
  initStorage conn graph
  run conn q1 ()
  id1 <- lastInsertedRowId conn -- TODO: ditto
  run conn q2 ()
  id2 <- lastInsertedRowId conn -- TODO: ditto
  run conn q3 ()
  id3 <- lastInsertedRowId conn -- TODO: ditto
  run conn q4 [id1, id2, id3]
  tid <- lastInsertedRowId conn -- TODO: ditto
  t <- loadTriple conn graph tid
  disconnect conn
  case t of
    Nothing -> return $ testCase "loadTriple test" $ HU.assert False
    Just tr -> return $ testCase "loadTriple test" $ HU.assert $ expectedTriple == tr
      where expectedTriple = triple (unode "http://example.org/rdf/1") (unode "http://example.org/rdf/2") (unode "http://example.org/rdf/3")
  where graph = "load_triple_test"
        q1 = Query $ TL.pack $ "INSERT INTO `" ++ (T.unpack graph) ++ "_nodes` (type, text) VALUES ('UNode', 'http://example.org/rdf/1')"
        q2 = Query $ TL.pack $ "INSERT INTO `" ++ (T.unpack graph) ++ "_nodes` (type, text) VALUES ('UNode', 'http://example.org/rdf/2')"
        q3 = Query $ TL.pack $ "INSERT INTO `" ++ (T.unpack graph) ++ "_nodes` (type, text) VALUES ('UNode', 'http://example.org/rdf/3')"
        q4 = Query $ TL.pack $ "INSERT INTO `" ++ (T.unpack graph) ++ "_triples` (subject_id, predicate_id, object_id) VALUES (?, ?, ?)"

loadRDFTest :: IO Test
loadRDFTest = do
  conn <- connectSqlite3 testDatabase
  initStorage conn graph
  run conn q1 ()
  id1 <- lastInsertedRowId conn -- TODO: ditto
  run conn q2 ()
  id2 <- lastInsertedRowId conn -- TODO: ditto
  run conn q3 ()
  id3 <- lastInsertedRowId conn -- TODO: ditto
  run conn q4 [id1, id2, id3]
  rdf <- loadRDF conn graph :: IO TriplesGraph
  disconnect conn
  return $ testCase "loadRDF test" $ HU.assert $ (triplesOf expectedRDF) == (triplesOf rdf)
  where graph = "load_rdf_test"
        q1 = Query $ TL.pack $ "INSERT INTO `" ++ (T.unpack graph) ++ "_nodes` (type, text) VALUES ('UNode', 'http://example.org/rdf/1')"
        q2 = Query $ TL.pack $ "INSERT INTO `" ++ (T.unpack graph) ++ "_nodes` (type, text) VALUES ('UNode', 'http://example.org/rdf/2')"
        q3 = Query $ TL.pack $ "INSERT INTO `" ++ (T.unpack graph) ++ "_nodes` (type, text) VALUES ('UNode', 'http://example.org/rdf/3')"
        q4 = Query $ TL.pack $ "INSERT INTO `" ++ (T.unpack graph) ++ "_triples` (subject_id, predicate_id, object_id) VALUES (?, ?, ?)"
        expectedRDF = mkRdf ts baseUri pms :: TriplesGraph
        ts = [ triple (unode "http://example.org/rdf/1") (unode "http://example.org/rdf/2") (unode "http://example.org/rdf/3") ]
        baseUri = Nothing
        pms = PrefixMappings Map.empty


-- AUXILIARY

initStorage :: Connection c => c -> T.Text -> IO ()
initStorage conn graph = do
  deleteStorage conn graph
  createStorage conn graph

-- NB: Copied from Persistence.hs, read comments there.
lastInsertedRowId :: Connection c => c -> IO SqlValue
lastInsertedRowId conn = do
  r <- runFetch conn query ()
  case r of
      Nothing -> return $ SqlInteger 0
      Just i  -> return i
  where query = Query $ TL.pack "select last_insert_rowid()"
