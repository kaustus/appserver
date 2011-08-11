(ns pgconnect.logic
  (:use pgconnect.dbwork)
  (:use (incanter core excel))
  (:import [java.util.Date])
  (:require [clojure.string :as str]
            [clojure.contrib.json :as json2]
            ;[org.danlarkin.json :as json]
            )) 

(defn change-instanse-tostring
  [instance result]
  (first
   (vector
	(reduce (fn [acc [k v]]
                      (assoc acc k
                             (if (instance? instance v)
                               (str v)
                               v))) {} (first result)))))

(defn change-instanse-tostring2
  [instance result]
  (reduce (fn [acc [k v]]
                      (assoc acc k
                             (if (instance? instance v)
                               (str v)
                               v))) {} result))

(defn auth [params]
  (let [name (params "name")
		pass (params "pass")]
	(if (= (:pass (first (db-get-user name)))
		   pass)
	  "OK"
	  "FAIL")))

(defn make-report
  []
  (let [datamap (change-instanse-tostring java.sql.Date
										  (db-get-liftdata "009"))]
	(save-xls (dataset (map #(name %) (keys datamap)) (vector (map #(if (nil? %) "" %) (vals datamap))))
			  "c:/report.xls")))

(defn get-regnumdata
  [regnum]
  (let [rn regnum ;(second (str/split regnum #"\$"))
        result (db-get-liftdata rn)]
    (json2/json-str
	 (vector
		   (change-instanse-tostring java.sql.Date result)))))

(defn get-test
  []
  (json2/json-str
  (:street (db-get-test-data))))

(defn get-regnums
  []
  (json2/json-str
   (into []
		 (for [rn (db-get-all-regnums)]
				(change-instanse-tostring2 java.sql.Date rn)))))

(defn get-addresses
  []
  (json2/json-str
   (into [] (for [addr (db-get-all-addr)]
              addr))))

(defn get-brigades
  []
  (json2/json-str
   (into [] (for [brigade (db-get-all-brigades)]
              (:brigade brigade)))))

(defn get-contracts
  []
  (json2/json-str
   (into [] (for [contract (db-get-all-contracts)]
              (:num_contract contract)))))

(defn get-brigadework
  [brigade]
  (json2/json-str
   (into [] (for [addr (db-get-brigade-addresses brigade)]
              addr))))

(defn get-contractwork
  [contract]
  (json2/json-str
   (into [] (for [addr (db-get-contract-addresses contract)]
              addr))))

(defn get-regions
  []
  (json2/json-str
   (into [] (for [region (db-get-regions)]
              (:region_name region)))))

(defn get-sel-regnums
  [regnum_]
  (json2/json-str
   (into [] (for [rn (db-get-like-regnums regnum_)]
              (change-instanse-tostring2 java.sql.Date rn)))))

(defn get-sel-addrs
  [addr]
  (json2/json-str
   (into [] (for [ad (db-get-like-addrs addr)]
             ad))))

(defn get-sel-region
  [region]
  (json2/json-str
   (into [] (for [ad (db-get-sel-region region)]
             ad))))

(defn get-sel-expertise
  [regnum]
  (let [result (db-get-exp regnum)]
    (json2/json-str
	 (vector
	  (change-instanse-tostring java.sql.Date result)))))

(defn get-sel-spec
  [regnum_]
  (let [result (db-get-spec regnum_)]
    (json2/json-str
	 (vector
	  (change-instanse-tostring java.sql.Date result)))))
;     (into [] result))))

(defn get-sel-contract
  [contract]
  (let [result (db-get-contract contract)]
    (json2/json-str
     (vector
	  (change-instanse-tostring java.sql.Date result)))))

(defn get-brigade-data
  [brigade]
  (let [result (db-get-brigade-data brigade)]
    (json2/json-str
     (into [] result))))

;;
;; requests for full select form
;;

;; main select

(defn get-fullselect-data
  [params]
  (json2/json-str
   (into [] (for [rn (db-get-full-data params)]
             (change-instanse-tostring2 java.sql.Date rn)))))

;; secondary

(defn get-streets
  []
  (json2/json-str
   (into [] (for [street (db-get-streets)]
              (:street_name street)))))

(defn get-houses
  []
  (json2/json-str
   (into [] (for [house (db-get-houses)]
              (:house house)))))

(defn get-holders
  []
  (json2/json-str
   (into [] (for [holder (db-get-holders)]
              (:holder holder)))))

(defn get-belongs
  []
  (json2/json-str
   (into [] (for [belong (db-get-belongs)]
              (:belong belong)))))

(defn get-places
  []
  (json2/json-str
   (into [] (for [place (db-get-places )]
              (:place place )))))

(defn get-buildings
  []
  (json2/json-str
   (into [] (for [btype (db-get-buildings)]
              (:building_name btype )))))

(defn get-makers
  []
  (json2/json-str
   (into [] (for [maker (db-get-makers)]
              (:maker_name maker )))))

(defn get-lifttypes
  []
  (json2/json-str
   (into [] (for [ltype (db-get-lifttypes)]
              (:lifttype ltype )))))

(defn get-speeds
  []
  (json2/json-str
   (into [] (for [speed (db-get-speeds)]
              (:speed speed)))))

(defn get-weights
  []
  (json2/json-str
   (into [] (for [w (db-get-weights)]
              (:weight w)))))

(defn get-doortypes
  []
  (json2/json-str
   (into [] (for [dtype (db-get-doortypes)]
              (:door_type dtype)))))

(defn get-fences
  []
  (json2/json-str
   (into [] (for [fence (db-get-fences)]
              (:cab_fense fence)))))

(defn get-mines []
  (json2/json-str (into [] (for [mine (db-get-mines)]
                             (:mine mine )))))

(defn get-mp-places []
  (json2/json-str (into [] (for [mpp (db-get-mp-places)]
                             (:mp_location mpp)))))

(defn get-mp-enters []
  (json2/json-str (into [] (for [mpe (db-get-mp-enters)]
                             (:mp_enter mpe)))))

(defn get-links []
  (json2/json-str (into [] (for [link (db-get-links)]
                             (:link link)))))

(defn get-linktypes []
  (json2/json-str (into [] (for [lt (db-get-linktypes)]
                             (:link_pult_type lt)))))

(defn get-linkpults []
  (json2/json-str (into [] (for [lp (db-get-linkpults)]
                             (:link_pult lp)))))

(defn get-panels []
  (json2/json-str (into [] (for [p (db-get-panels)]
                             (:panel p)))))

(defn get-comcirs []
  (json2/json-str (into [] (for [cc (db-get-comcirs)]
                             (:comcir cc)))))

(defn get-circuits []
  (json2/json-str (into [] (for [c (db-get-circuits)]
                             (:circuit c)))))

(defn get-reductors []
  (json2/json-str (into [] (for [red (db-get-reductors)]
                             (:reductor red)))))

(defn get-transfers []
  (json2/json-str (into [] (for [transfer (db-get-transfers)]
                             (:transfer_num transfer)))))

(defn get-sets []
  (json2/json-str (into [] (for [set (db-get-sets)]
                             (:set_comp set)))))

(defn get-engines []
  (json2/json-str (into [] (for [engine (db-get-engines)]
                             (:engine engine)))))

(defn get-e-forms []
  (json2/json-str (into [] (for [eform (db-get-eforms)]
                             (:engine_form eform)))))

(defn get-doorengines []
  (json2/json-str (into [] (for [doorengine (db-get-doorengines)]
                             (:door_engine doorengine)))))

(defn get-de-forms []
  (json2/json-str (into [] (for [deform (db-get-deforms)]
                             (:door_engine_form deform)))))

(defn get-kvdiams []
  (json2/json-str (into [] (for [kvd (db-get-kvdiams)]
                             (:kvdiam kvd)))))

(defn get-kvnums []
  (json2/json-str (into [] (for [kvn (db-get-kvnums)]
                             (:kvnum kvn)))))

(defn get-blockdiams []
  (json2/json-str (into [] (for [bd (db-get-blockdiams)]
                             (:blockdiam bd)))))

(defn get-ropediams []
  (json2/json-str (into [] (for [rd (db-get-ropediams)]
                             (:rope_diam rd)))))

(defn get-ropelens []
  (json2/json-str (into [] (for [rl (db-get-ropelens)]
                             (:rope_len rl)))))

(defn get-breaks []
  (json2/json-str (into [] (for [break (db-get-breaks)]
                             (:break_magnet break)))))

(defn get-blocks []
  (json2/json-str (into [] (for [block (db-get-blocks)]
                             (:block block)))))

(defn get-constructs []
  (json2/json-str (into [] (for [cons (db-get-constructs)]
                             (:construct cons)))))

(defn get-controls []
  (json2/json-str (into [] (for [control (db-get-controls)]
                             (:load_control control)))))

(defn get-balances []
  (json2/json-str (into [] (for [balance (db-get-balances)]
                             (:balance balance)))))

;; update

(defn update-maindata
  [params]
  (db-update-maindata params)
  "OK")

(defn update-techdata
  [params]
  (db-update-techdata params)
  "OK")

(defn update-contractdata
  [params]
  (db-update-contractdata params)
  "OK")

(defn update-brigadedata
  [params]
  (db-update-brigadedata params)
  "OK")

(defn update-expertisedata
  [params]
  (db-update-expertisedata params)
  "OK")

(defn insert-newval
  [params]
  (db-insert-newval params)
  "OK")
