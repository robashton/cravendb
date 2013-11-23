# Querying

Craven automatically indexes content that goes into it using a set of heuristics based around size/types/etc. This means that arbitrary queries on the documents stored inside Craven should mostly be *relatively* fast.

There is an ability to customise this process, but this is remaining a closed and undocumented API until it is requested for a good reason. This whole section is *definitely* subject to change, and can be considered as temporary at best.

So we've got a database called *instance*, and we've put a load of documents in it which are permutations of this wonderful thing here...

```clojure
{
  :name "Pinkie Pie"
  :description "Pinkie Pie is the best pony ever and anybody who disagrees better think that about Rainbow Dash instead"
  :cutiemark "Balloons"
  :colour :pink
  :bestpony: true
  :episodes 133
  :catchphases [ "this is the best day ever" "let's have a party" ]
}
```

There is a namespace to help build queries, bringing this in is usually a good idea.

```clojure
(require '[cravendb.querylanguage :refer :all])
```

### Our first query

*Let's find all the pink ponies*
```clojure
(db/query instance (=? :colour :pink))
```

*Let's find all the ponies that were in more than 10 episodes*
```clojure
(db/query instance (>? :episodes 10))
```

*How about any pony with a catch-phrase with "party" in it*
```clojure
(db/query instance (has-word? :catchphrases "party"))
```

We can combine queries and perform negations with the standard AND/OR/NOTs 

*Pink ponies who know how to party*
```clojure
(db/query instance (AND (=? :colour :pink) (has-word? :catchphrases "party")))
```

### A complete list of these conditions

```clojure
; Is k greater than v?
(>? [k v])
; Is k smaller than v?
(<? [k v]) 
; Is k equal to v?
(=? [k v]) 
; Is k greater than or equal to v?
(>=? [k v]) 
; is k smaller than or equal to v?
(<=? [k v]) 
; Does k start with v?
(starts-with? [k v]) 
; Does k have the word 'v' in it?
(has-word? [k v])
; Does k have a word starting with v?
(has-word-starting-with? [k v])
; Does the collection k have value v?
(has-item? [k v])
; Are all of these conditions true?
(AND [& v])
; Are any of these conditions true?
(OR [& v])
; This condition must not be true
(NOT [& v])
```

## Feedback

- Suggestions for things to add, please do!
- Not working as you expect? Please tell us!
- Think we can be more idiomatic? Tell us how!
