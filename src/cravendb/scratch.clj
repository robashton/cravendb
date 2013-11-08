(ns cravendb.scratch
  "The sole purpose of this file is to act as a place to play with stuff in repl"
  (:use [cravendb.testing]
        [cravendb.core]
        [clojure.tools.logging :only (info debug error)]
        [clojure.core.async])
  (:import (java.io File File PushbackReader IOException FileNotFoundException ))
  (:require [clojure.edn :as edn]))

