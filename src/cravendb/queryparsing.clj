(ns cravendb.queryparsing
  (:require [instaparse.core :as insta])
  (:require [clojure.tools.logging :refer [info error debug]])
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
    <Function> = <'('>  (LessThanCall | GreaterThanCall | GreaterThanOrEqualCall | LessThanOrEqualCall | 
                            AndCall | OrCall | EqualsCall | NotEqualsCall | ContainsCall | StartsWithCall )  <')'>   
    <Argument> = (Function | LiteralValue)

    AndCall = <'and'> (<Whitespace> Argument)*
    OrCall = <'or'> (<Whitespace> Argument )*
    EqualsCall = <'='> <Whitespace> FieldName <Whitespace> LiteralValue
    LessThanCall = <'<'> <Whitespace> FieldName <Whitespace> LiteralValue
    GreaterThanCall = <'>'> <Whitespace> FieldName <Whitespace> LiteralValue
    LessThanOrEqualCall = <'<='> <Whitespace> FieldName <Whitespace> LiteralValue
    GreaterThanOrEqualCall = <'>='> <Whitespace> FieldName <Whitespace> LiteralValue
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

(defn create-less-than-clause [[field-type field-name] [value-type value-value]]
  (case value-type
    :NumericValue (NumericRangeQuery/newIntRange field-name (int -10000000) (Integer/parseInt value-value) false false)))

(defn create-greater-than-clause [[field-type field-name] [value-type value-value]]
  (case value-type
    :NumericValue (NumericRangeQuery/newIntRange field-name (Integer/parseInt value-value) (int 10000000) false false)))

(defn create-less-than-or-equal-clause [[field-type field-name] [value-type value-value]]
  (case value-type
    :NumericValue (NumericRangeQuery/newIntRange field-name (int -10000000) (Integer/parseInt value-value) true true)))

(defn create-greater-than-or-equal-clause [[field-type field-name] [value-type value-value]]
  (case value-type
    :NumericValue (NumericRangeQuery/newIntRange field-name (Integer/parseInt value-value) (int 10000000) true true)))

(defn create-wildcard [in]
  (MatchAllDocsQuery.))

(defn to-lucene [query]
  (debug "Interpreting" query)
  (first (drop 1 (insta/transform 
    {
     :S nil
     :EqualsCall create-equals-clause 
     :LessThanCall create-less-than-clause
     :GreaterThanCall create-greater-than-clause
     :GreaterThanOrEqualCall create-greater-than-or-equal-clause
     :LessThanOrEqualCall create-less-than-or-equal-clause
     :StartsWithCall create-starts-with-clause
     :Wildcard create-wildcard
     }
    (query-parser query)))))
