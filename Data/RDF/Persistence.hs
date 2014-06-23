{-# LANGUAGE OverloadedStrings #-}

-- TODO:
-- Wrap all SQL queries into transactions, make sure lastInsertedRowId is valid!
-- Implement complete RDF->SQL and SQL->RDF serializing.
-- Unique (row/node) constraints?

module Data.RDF.Persistence where

import Data.RDF
import Database.HDBI
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL

type GraphName = T.Text

-- Create the tables required for the persistence to work.
--
-- User is expected to initialize a connection instance on his own
-- (e.g. using "connectSqlite3" from hdbi-sqlite package)
-- and pass it as argument here.
--
-- DB tables layout:
--
-- Triples:
--    id, subject_id, predicate_id, object_id
-- Nodes:
--    id, type, text
-- where "type" is one of Nodes: UNode, BNode, LNode
--
createStorage :: Connection c => c -> GraphName -> IO ()
createStorage conn graph = do
  run conn (Query $ TL.pack $ "CREATE TABLE `" ++ (T.unpack graph) ++ "_triples` (id INTEGER PRIMARY KEY NOT NULL, subject_id INTEGER, predicate_id INTEGER, object_id INTEGER)") ()
  run conn (Query $ TL.pack $ "CREATE TABLE `" ++ (T.unpack graph) ++ "_nodes` (id INTEGER PRIMARY KEY NOT NULL, type STRING, text STRING, tag STRING)") ()

nodeToSqlValues :: Node -> [(T.Text, SqlValue)]
nodeToSqlValues (UNode uri)  = [("type", SqlText "UNode"), ("text", toSql uri)]
nodeToSqlValues (LNode v)    = case v of
                                 PlainL tx -> [("type", SqlText "PlainL"), ("text", toSql tx)]
                                 PlainLL tx ln -> [("type", SqlText "PlainLL"), ("text", toSql tx), ("tag", toSql ln)]
                                 TypedL tx tp -> [("type", SqlText "TypedL"), ("text", toSql tx), ("tag", toSql tp)]
nodeToSqlValues (BNode nid)  = [("type", SqlText "BNode"), ("text", toSql nid)]
nodeToSqlValues (BNodeGen nid) = [("type", SqlText "BNodeGen"), ("text", toSql $ show nid)]

storeNode :: Connection c => c -> GraphName -> Node -> IO SqlValue
storeNode conn graph n = do
  runQuery $ (unzip . nodeToSqlValues) n
  lastInsertedRowId conn
  where runQuery (cols, vals) = run conn query vals
            where query = (Query . TL.pack . T.unpack) $ T.concat [
                           "INSERT INTO `",
                           graph,
                           "_nodes` (",
                           (T.intercalate ", " cols),
                           ") VALUES (",
                           (T.intercalate ", " $ replicate (length cols) "?"),
                           ")" ]

storeTriple :: Connection c => c -> GraphName -> Triple -> IO SqlValue
storeTriple conn graph (Triple subj pred obj) = do
  subjId <- storeNode conn graph subj
  predId <- storeNode conn graph pred
  objId  <- storeNode conn graph obj
  run conn query [subjId, predId, objId]
  lastInsertedRowId conn
  where query = Query $ TL.pack ("INSERT INTO " ++ (T.unpack graph) ++ "_triples (subject_id, predicate_id, object_id) VALUES (?, ?, ?)")

storeRDF :: (Connection c, RDF rdf) => c -> GraphName -> rdf -> IO Bool
storeRDF conn graph rdf = do
  xs <- mapM (storeTriple conn graph) $ triplesOf rdf
  return $ all (\x -> x /= SqlInteger 0) xs


-- TODO: This is actually a connection-specific function,
-- which must be implemented in its appropriate adapter individually
-- (e.g. "hdbi-sqlite" should use SQlite version of getting the last inserted row id)
lastInsertedRowId :: Connection c => c -> IO SqlValue
lastInsertedRowId conn = do
  r <- runFetch conn query ()
  case r of
      Nothing -> return $ SqlInteger 0
      Just i  -> return i
  where query = Query $ TL.pack "select last_insert_rowid()"
