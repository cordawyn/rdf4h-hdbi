{-# LANGUAGE OverloadedStrings #-}

module Data.RDF.Persistence (
  createStorage, deleteStorage,
  storeNode, storeTriple, storeRDF,
  loadNode, loadTriple, loadTripleByIds, loadRDF
) where

import Data.RDF
import Database.HDBI
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL (pack)
import qualified Data.Foldable as F (concatMap)
import Data.Functor ((<$>))
import Control.Applicative ((<*>))
import qualified Data.Map as Map (empty)
import Data.Maybe (catMaybes)

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
--    id, type, text, tag
-- where "type" is one of Nodes: UNode, BNode, LNode
createStorage :: Connection c => c -> GraphName -> IO ()
createStorage conn graph = do
  run conn (Query $ TL.pack $ "CREATE TABLE IF NOT EXISTS `" ++ (T.unpack graph) ++ "_triples` (id INTEGER PRIMARY KEY NOT NULL, subject_id INTEGER, predicate_id INTEGER, object_id INTEGER, UNIQUE(subject_id, predicate_id, object_id))") ()
  run conn (Query $ TL.pack $ "CREATE TABLE IF NOT EXISTS `" ++ (T.unpack graph) ++ "_nodes` (id INTEGER PRIMARY KEY NOT NULL, type STRING, text STRING, tag STRING, UNIQUE(type, text, tag))") ()

-- Delete the RDF persistence tables.
deleteStorage :: Connection c => c -> GraphName -> IO ()
deleteStorage conn graph = do
  run conn (Query $ TL.pack $ "DROP TABLE IF EXISTS `" ++ (T.unpack graph) ++ "_nodes`") ()
  run conn (Query $ TL.pack $ "DROP TABLE IF EXISTS `" ++ (T.unpack graph) ++ "_triples`") ()

nodeToSqlValues :: Node -> [(T.Text, SqlValue)]
nodeToSqlValues (UNode uri)    = [("type", SqlText "UNode"), ("text", toSql uri)]
nodeToSqlValues (BNode nid)    = [("type", SqlText "BNode"), ("text", toSql nid)]
nodeToSqlValues (BNodeGen nid) = [("type", SqlText "BNodeGen"), ("text", toSql $ show nid)]
nodeToSqlValues (LNode v)      = case v of
                                   PlainL tx -> [("type", SqlText "PlainL"), ("text", toSql tx)]
                                   PlainLL tx ln -> [("type", SqlText "PlainLL"), ("text", toSql tx), ("tag", toSql ln)]
                                   TypedL tx tp -> [("type", SqlText "TypedL"), ("text", toSql tx), ("tag", toSql tp)]

-- Store an RDF Node in the database.
storeNode :: Connection c => c -> GraphName -> Node -> IO SqlValue
storeNode conn graph n = withTransaction conn $ do
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

-- Store an RDF Triple in the database.
-- (Stores all dependent nodes as well).
-- FIXME: Make sure that stored triples refer proper node Ids,
-- if a node duplicate is already present in the database.
storeTriple :: Connection c => c -> GraphName -> Triple -> IO SqlValue
storeTriple conn graph (Triple subj pred obj) = do
  subjId <- storeNode conn graph subj
  predId <- storeNode conn graph pred
  objId  <- storeNode conn graph obj
  withTransaction conn $ do
    run conn query [subjId, predId, objId]
    lastInsertedRowId conn
    where query = Query $ TL.pack ("INSERT INTO " ++ (T.unpack graph) ++ "_triples (subject_id, predicate_id, object_id) VALUES (?, ?, ?)")

-- Store a whole RDF graph in the database.
-- Note that due to uniqueness constraints,
-- the database may not appear to "mirror" your original graph.
-- TODO: BaseURI and PrefixMappings are not stored.
storeRDF :: (Connection c, RDF rdf) => c -> GraphName -> rdf -> IO Bool
storeRDF conn graph rdf = do
  xs <- mapM (storeTriple conn graph) $ triplesOf rdf
  return $ all (\x -> x /= SqlInteger 0) xs

-- Load Node from the database by its Id.
loadNode :: Connection c => c -> GraphName -> SqlValue -> IO (Maybe Node)
loadNode conn graph nid = do
  nsql <- runFetch conn query [nid]
  case nsql of
    Nothing -> return Nothing
    Just row  -> return $ Just (sqlValuesToNode row)
  where query = Query $ TL.pack $ "SELECT * FROM `" ++ (T.unpack graph) ++ "_nodes` WHERE id = ? LIMIT 1"

-- Convert a list of SqlValues (SQL row) into a node.
-- The first column (id) is ignored.
-- Raises exceptions on unknown node types or invalid input format.
sqlValuesToNode :: [SqlValue] -> Node
sqlValuesToNode [_, ntype, ntext, ntag] =
    case (fromSql ntype :: T.Text) of
      "UNode" -> unode $ fromSql ntext
      "BNode" -> bnode $ fromSql ntext
      "BNodeGen" -> BNodeGen (read (fromSql ntext) :: Int)
      "PlainL" -> lnode $ plainL $ fromSql ntext
      "PlainLL" -> lnode $ plainLL (fromSql ntext) (fromSql ntag)
      "TypedL" -> lnode $ typedL (fromSql ntext) (fromSql ntag)
      _ -> error $ "Unknown node type in SQL: " ++ (fromSql ntype)
sqlValuesToNode _ = error "Invalid SQL format for Node"

-- Load Triple from the database by its Id.
loadTriple :: Connection c => c -> GraphName -> SqlValue -> IO (Maybe Triple)
loadTriple conn graph tid = do
  tsql <- runFetch conn query [tid]
  case tsql of
    Nothing -> return Nothing
    Just [_, subjId, predId, objId] -> loadTripleByIds conn graph subjId predId objId
    Just _ -> error "Invalid SQL format for Triple"
  where query = Query $ TL.pack $ "SELECT * FROM `" ++ (T.unpack graph) ++ "_triples` WHERE id = ? LIMIT 1"

-- Load Triple from the database, given the Ids of its subject, predicate and object nodes.
loadTripleByIds :: Connection c => c -> GraphName -> SqlValue -> SqlValue -> SqlValue -> IO (Maybe Triple)
loadTripleByIds conn graph subjId predId objId = do
    newTriple <$> loadNode conn graph subjId <*> loadNode conn graph predId <*> loadNode conn graph objId
    where newTriple s p o = triple <$> s <*> p <*> o

-- Load an RDF graph from the database.
-- TODO: BaseURI and PrefixMappings are not loaded.
loadRDF :: (Connection c, RDF rdf) => c -> GraphName -> IO rdf
loadRDF conn graph = do
  tsql <- runFetchAll conn query ()
  ts <- sequence $ F.concatMap (\t -> [newTriple t]) tsql
  return $ mkRdf (catMaybes ts) baseUri pms
  where query = Query $ TL.pack $ "SELECT * FROM `" ++ (T.unpack graph) ++ "_triples`"
        newTriple [_, subjId, predId, objId] = loadTripleByIds conn graph subjId predId objId
        newTriple _ = error "Invalid SQL format for Triple"
        baseUri = Nothing -- TODO
        pms = PrefixMappings Map.empty -- TODO


-- AUXILIARY

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
