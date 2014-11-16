(ns log-truncate.core
  (:gen-class)
  (:import (java.io RandomAccessFile File)
           (java.nio.file FileSystem StandardOpenOption)
           (java.nio.channels FileChannel)
           (java.util Collections)
           (java.util.regex Matcher)
           (org.joda.time DateTime)
           (java.nio.file.attribute FileAttribute))
  (:require [clj-time.core :as t]
            [clj-time.local :as l])
  (:refer-clojure))

;;  16kb 64kb 256kb 1MB 4MB 16MB 64MB
(def size-vec
  (loop [cnt 0 seq [] val (* 16 1024)]
    (if (= cnt 8)
      seq
      (recur (+ cnt 1) (conj seq (long val)) (bit-shift-left val 2)))))


(defn- find-skip-val
  "Given the lenght of file finds a appropriate skip val"
  [file-size]
  (let [slab (-> file-size (bit-shift-right 10) (bit-or 1) long)]
    (-> size-vec
         (Collections/binarySearch slab compare)
         (+ 2) (* -1) size-vec)))

(def buff (byte-array 1024))
;; used for running map on copy
(def copy-seq (range 31))
;; The date matcher regex pattern
(def pattern #"\n(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d+-\d+):")

(defn- buff-array-reset!
  "we copies the last 31 odd characters to the begining of the array
   this is needed because we might have the partial match in the end
   and next time we should start at the offset"
  [curr-offset]
  (if (< curr-offset 31)
    curr-offset
    (-> #(aset-byte buff % (aget buff (+ curr-offset -31 %)))
        (map copy-seq)
        dorun (do 31))))


(defn- get-matcher
  "Try to return a matcher for the buffer, you have to
   pass the amount to read from buffer"
  [buff-len]
  (->> buff (map char) (take buff-len) (apply str) (re-matcher pattern)))

(defn- execute-read-file
  "General pattern for reading into the file "
  [^RandomAccessFile file check-fn ]
  (loop [buff-offset 0]
    (let [read-cnt (.read file buff buff-offset (- 1024 buff-offset))
          buff-len (+ read-cnt buff-offset)]
      (if (>= read-cnt 0)
        (if-let [resp (check-fn file buff-len)]
          resp
          (-> buff-len buff-array-reset! recur))))))

(defn- find-first-time
  "Finds the first time pattern available in the particular block of the file"
  [^RandomAccessFile file]
  (execute-read-file file
    (fn [_ buff-len]
      (if-let [match (-> buff-len get-matcher re-find)]
        (-> match second l/to-local-date-time)))))

(defn- scan-check-logic
  "The helper for checking the logic in the scan range function"
  [^DateTime time ^RandomAccessFile file buff-len]
  (let [^Matcher matcher (get-matcher buff-len)
        compare-fn #(-> % second l/to-local-date-time
                        (compare time) (>= 0))
        get-start-offset #(->> matcher .start (* -1)
                              (- (.getFilePointer file) buff-len))]
    (loop [match (re-find matcher)]
      (cond (nil? match) nil
            (compare-fn match) (get-start-offset)
            :else (-> matcher re-find recur)))))

(defn- scan-range!
  "Finds the first time range that is greater than
   the given time and returns the start offset of that time"
  [^RandomAccessFile file ^DateTime time]
  (if-let [val (execute-read-file file (partial scan-check-logic time))]
    val (-> file .length (- 1))))

(defn- find-range
  "Finds the range of bytes to copy and
   return a map containing the ranges"
  [^RandomAccessFile file ^String start ^String end]
  (let [low (l/to-local-date-time start)
        high (l/to-local-date-time end)
        skip-length (-> file .length find-skip-val)
        scan-range (partial scan-range! file)
        file-len (+ (.length file) 0.0)]
    (println "skip-len val " skip-length)
    (loop [ptr 0 res {} stage -1]
      (.seek file ptr)
      ;;(println "Completion ratio:" (/ ptr file-len))
      (let [first-date (find-first-time file)
            date (if (zero? stage) high low)]
        (if (> (compare date first-date) 0)
          (recur (+ ptr skip-length) res stage)
          (do (.seek file (- ptr skip-length))
              (case stage
                -1 (let [new-res (assoc res :start (scan-range low))]
                     (if (<= (compare high first-date) 0)
                       (do (.seek file (:start new-res))
                           (assoc new-res :end (scan-range high)))
                       (recur (+ skip-length ptr) new-res 0)))
                0 (assoc res :end (scan-range high)))))))))

(defn- copy-files
  "Copies from the first file into the second
   file only for the range of bytes given"
  [^RandomAccessFile src ^String dest {:keys [start end]}]
  (let [dest-channel (-> dest File. .toPath
                         (FileChannel/open
                           #{StandardOpenOption/CREATE
                             StandardOpenOption/WRITE
                             StandardOpenOption/TRUNCATE_EXISTING}
                           (into-array FileAttribute [])))
        src-channel (.getChannel src)]
    (loop [start start len (- end start -1)]
      (when (> len 0)
        (let [count (.transferTo src-channel start len dest-channel)]
          (println "Start " start " : " len " : " count)
          (when (>= count 0)
            (recur (+ start count) (- len count))))))
    (.close dest-channel)))

(defn -main
  "Main glue"
  [src-file dest-file time-low time-high]
  (let [src (RandomAccessFile. src-file "r")]
    (->> (find-range src time-low time-high)
         (copy-files src dest-file))
    (println "Finished copying the file range")
    (.close src)))
