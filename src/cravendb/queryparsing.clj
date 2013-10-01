(ns cravendb.queryparsing
  (:require [instaparse.core :as insta])
  (:import 
           (org.apache.lucene.index Term)
           (org.apache.lucene.search TermQuery NumericRangeQuery PrefixQuery
                                     MatchAllDocsQuery)
           (org.apache.lucene.document Document Field Field$Store Field$Index 
                                      TextField IntField FloatField StringField)))


(def query-parser 
  (insta/parser
    "S = (Function | Wildcard) 
    Wildcard = '*'
    Whitespace = #'\\s+'
    <Function> = <'('>  (AndCall | OrCall | EqualsCall | NotEqualsCall | ContainsCall | StartsWithCall )  <')'>   
    <Argument> = (Function | LiteralValue)

    AndCall = <'and'> (<Whitespace> Argument)*
    OrCall = <'or'> (<Whitespace> Argument )*
    EqualsCall = <'='> <Whitespace> FieldName <Whitespace> LiteralValue
    StartsWithCall = <'starts-with'> <Whitespace> FieldName <Whitespace> LiteralValue
    NotEqualsCall = <'not='> <Whitespace> FieldName <Whitespace> LiteralValue
    ContainsCall = <'contains'> <Whitespace> FieldName <Whitespace> StringValue

    <LiteralValue> = (NumericValue | StringValue)
    <FieldName> =  (StringValue | Symbol)
    
    Symbol =  #':([a-zA-Z]+)'
    StringValue = <'\"'> #'[a-zA-Z]+' <'\"'>
    NumericValue = #'[0-9]+' "
  ))

(defn create-equals-clause [[field-type field-name] [value-type value-value] ]
  (case value-type
    :StringValue (TermQuery. (Term. field-name value-value))
    :NumericValue (NumericRangeQuery/newIntRange field-name (Integer/parseInt value-value) (Integer/parseInt value-value) true true) ))

(defn create-starts-with-clause [[field-type field-name] [value-type value-value]]
  (case value-type
    :StringValue (PrefixQuery. (Term. field-name value-value))))

(defn create-wildcard [in]
  (MatchAllDocsQuery.))

(defn extract-query [q] q)

(defn to-lucene [query]
  (first (drop 1(insta/transform 
    {
     :S nil
     :EqualsCall create-equals-clause 
     :StartsWithCall create-starts-with-clause
     :Wildcard create-wildcard
     }
    (query-parser query)))))

