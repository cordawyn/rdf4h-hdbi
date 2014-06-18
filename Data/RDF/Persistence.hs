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

-- Convert a node to a partial SQL syntax that can be used in "INSERT INTO ..." statements.
-- TODO: SQL-escape the text from the nodes (use "toSql"?).
nodeToSQL :: Node -> TL.Text
nodeToSQL (UNode n) = TL.pack $ "(type, text) VALUES ('UNode', '" ++ (T.unpack n) ++ "')"
nodeToSQL (LNode n) = case n of
                        PlainL tx -> TL.pack $ "(type, text) VALUES ('PlainL', '" ++ (T.unpack tx) ++ "')"
                        PlainLL tx ln -> TL.pack $ "(type, text, tag) VALUES ('PlainLL', '" ++ (T.unpack tx) ++ "', '" ++ (T.unpack ln) ++ "')"
                        TypedL tx tp -> TL.pack $ "(type, text, tag) VALUE ('TypedL', '" ++ (T.unpack tx) ++ "', '" ++ (T.unpack tp) ++ "')"
nodeToSQL (BNode n) = TL.pack $ "(type, text) VALUES ('BNode', '" ++ (T.unpack n) ++ "')"
nodeToSQL (BNodeGen n) = TL.pack $ "(type, text) VALUES ('BNode', '" ++ (show n) ++ "')"

storeNode :: Connection c => c -> GraphName -> Node -> IO SqlValue
storeNode conn graph n = do
  run conn query ()
  lastInsertedRowId conn
  where query = Query $ TL.concat [TL.pack ("INSERT INTO `" ++ (T.unpack graph) ++ "_nodes` "), (nodeToSQL n)]

storeTriple :: Connection c => c -> GraphName -> Triple -> IO SqlValue
storeTriple conn graph (Triple subj pred obj) = do
  subjId <- storeNode conn graph subj
  predId <- storeNode conn graph pred
  objId  <- storeNode conn graph obj
  runMany conn query [subjId, predId, objId]
  lastInsertedRowId conn
  where query = Query $ TL.pack ("INSERT INTO " ++ (T.unpack graph) ++ "_triples (subject_id, predicate_id, object_id) VALUES (?, ?, ?)")

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
