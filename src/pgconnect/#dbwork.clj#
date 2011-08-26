(ns pgconnect.dbwork
  (:require [clojureql.core :as cql]
            [clojure.contrib.sql :as sql]))

(def *db* {:classname "org.postgresql.Driver"
           :subprotocol "postgresql"
           :subname "//127.0.0.1/radel"
           :user "postgres"
           :password "123"})

(def rn_proection
  [:liftnew.reg_num
   :liftnew.house
   :liftnew.corp
   :liftnew.parnum
   :regions.region_name
   :liftnew.floors
   :liftnew.stops
   :liftnew.speed
   :liftnew.weight
   :liftnew.place
   :liftnew.forbidden
   :liftnew.forbid_date
   :liftnew.forbid_reason
   :build_types.building_name
   :streets.street_name
   :is_served
   :contract_id
   :contracts.num_contract
   :contracts.holder
   :liftnew.brigade_id
   :liftnew.inspection
   :liftnew.inspector
   :liftnew.comment])

(def exp-projection
  [:last_exp_num
   :last_exp_date
   :next_exp_date
   :exp_maker
   :exp_maker_addr
   :exp_maker_phone
   :exp_story])

(defn is-like? [col text]
  (str col " LIKE '" text "%'"))

;(defn update-contract []
;  (

(defn db-get-user
  [name]
  @(cql/select
	(cql/table *db* :users)
	(cql/where (= :name name))))

(defn db-get-liftdata
  [regnum]
  @(cql/select
    (->
     (cql/join
      (cql/table *db* :liftnew)
      (cql/table *db* :regions)
      (cql/where (= :liftnew.region :regions.id_region)))
     (cql/join
      (cql/table *db* :streets)
      (cql/where (= :liftnew.street_id :streets.id_street)))
     (cql/join
      (cql/table *db* :build_types)
      (cql/where (= :liftnew.building_type :build_types.id_building)))
     (cql/join
      (cql/table *db* :contracts)
      (cql/where (= :liftnew.contract_id :contracts.id_contract)))
	 (cql/project rn_proection))
    (cql/where (= :reg_num regnum))))

(defn db-get-all-regnums
  []
  @(-> (cql/join
		(cql/table *db* :liftnew)
		(cql/table *db* :contracts)
		(cql/where (= :liftnew.contract_id :contracts.id_contract)))
       (cql/sort [:reg_num])
       (cql/project [:reg_num :weight :contracts.contract_end])
       (cql/distinct)))

(defn db-get-all-addr
  []
  @(-> (cql/join
        (cql/table *db* :liftnew)
        (cql/table *db* :streets)
        (cql/where (= :streets.id_street :liftnew.street_id))) 
       (cql/project [:liftnew.reg_num :streets.street_name :house :corp :parnum])
       (cql/sort [:streets.street_name])))

(defn db-get-regions
  []
  @(-> (cql/table *db* :regions)
       (cql/sort [:region_name])))

(defn db-get-all-brigades
  []
  @(-> (cql/table *db* :brigades)
       (cql/sort [:brigade])
       (cql/project [:brigade])))

(defn db-get-all-contracts
  []
  @(-> (cql/table *db* :contracts)
       (cql/sort [:num_contract])
       (cql/project [:num_contract])))

(defn db-get-like-regnums
  [regnum_]
  @(-> (cql/join
		(cql/table *db* :liftnew)
		(cql/table *db* :contracts)
		(cql/where (= :liftnew.contract_id :contracts.id_contract)))
       (cql/sort [:reg_num])
       (cql/project [:reg_num :weight :contracts.contract_end])
       (cql/select
        (cql/where (str "liftnew.reg_num LIKE '%" regnum_ "%'")))
       (cql/distinct)))

(defn db-get-like-addrs
  [addr]
  @(->
    (cql/join
     (cql/table *db* :liftnew)
     (cql/table *db* :streets)
     (cql/where (= :streets.id_street :liftnew.street_id)))
    (cql/sort [:streets.street_name])
    (cql/project [:liftnew.reg_num
                  :streets.street_name
                  :liftnew.house
                  :liftnew.corp
                  :liftnew.parnum])
    (cql/select
     (cql/where (str "streets.street_name LIKE '%" addr "%'")))))
  
(defn db-get-sel-region
  [region]
  @(-> (cql/join
        (cql/table *db* :liftnew)
        (cql/table *db* :regions)
        (cql/where (= :liftnew.region :regions.id_region)))
       (cql/join
        (cql/table *db* :streets)
        (cql/where (= :liftnew.street_id :streets.id_street)))
       (cql/select
        (cql/where (= region :regions.region_name)))
       (cql/sort [:streets.street_name])
       (cql/project [:liftnew.reg_num
                     :streets.street_name
                     :liftnew.house
                     :liftnew.corp
                     :liftnew.parnum])))

(defn db-get-exp
  [regnum]
  @(cql/select (-> (cql/table *db* :liftnew)
                   (cql/project exp-projection))
               (cql/where (= regnum :reg_num))))   

(defn db-get-spec
  [regnum]
  @(-> (cql/join
        (cql/table *db* :techspec)
        (cql/table *db* :liftmakers)
        (cql/where (= :techspec.maker_id :liftmakers.id_maker)))
       (cql/select
        (cql/where (= regnum :reg_num)))))   

(defn db-get-contract
  [contract]
  @(cql/select (cql/table *db* :contracts)
               (cql/where (= (Integer/parseInt contract) :id_contract))))   

(defn db-get-test-data
  []
  (sql/with-connection *db*
    (sql/with-query-results rs ["select * from liftnew"]
      (first rs))))

(defn db-get-brigade-data
  [brigade]
  @(cql/select (cql/table *db* :brigades)
               (cql/where (= :id_brigade (Integer/parseInt brigade)))))

(defn db-get-brigade-addresses
  [brigade]
  @(-> (cql/join
        (cql/table *db* :liftnew)
        (cql/table *db* :brigades)
        (cql/where (= :liftnew.brigade_id :brigades.id_brigade)))
       (cql/join
        (cql/table *db* :streets)
        (cql/where (= :liftnew.street_id :streets.id_street)))
       (cql/select
        (cql/where (= brigade :brigades.brigade)))
       (cql/project [:liftnew.reg_num
                     :streets.street_name
                     :liftnew.house
                     :liftnew.corp
                     :liftnew.parnum])))

(defn db-get-contract-addresses
  [contract]
  @(-> (cql/join
        (cql/table *db* :liftnew)
        (cql/table *db* :contracts)
        (cql/where (= :liftnew.contract_id
                      :contracts.id_contract)))
       (cql/join
        (cql/table *db* :streets)
        (cql/where (= :liftnew.street_id :streets.id_street)))
       (cql/select
        (cql/where (= :contracts.num_contract
                      contract)))
       (cql/project [:liftnew.reg_num
                     :streets.street_name
                     :liftnew.house
                     :liftnew.corp
                     :liftnew.parnum])))

;;;

(defn db-get-full-data
  [params]
  @(-> (cql/join
        (cql/table *db* :liftnew) (cql/table *db* :techspec)
        (cql/where (= :liftnew.reg_num :techspec.reg_num)))
       (cql/join (cql/table *db* :streets)
        (cql/where (= :liftnew.street_id :streets.id_street)))
       (cql/join (cql/table *db* :regions)
        (cql/where (= :liftnew.region :regions.id_region)))
       (cql/join (cql/table *db* :brigades)
        (cql/where (= :liftnew.brigade_id :brigades.id_brigade)))
       (cql/join (cql/table *db* :build_types)
        (cql/where (= :liftnew.building_type :build_types.id_building)))
       (cql/join (cql/table *db* :contracts)
        (cql/where (= :liftnew.contract_id :contracts.id_contract)))
       (cql/join (cql/table *db* :liftmakers)
        (cql/where (= :techspec.maker_id :liftmakers.id_maker)))
       (cql/select (cql/where
         (and
          (= :liftnew.is_served (if (= "true" (params "onserve")) true false))
          (if-let [street (params "street")]
            (= :streets.street_name street) (= 1 1))
          (if-let [house (params "house")]
            (= :liftnew.house house) (= 1 1))
          (if-let [holder (params "holder")]
            (= :contracts.holder holder) (= 1 1))
          (if-let [region (params "region")]
            (= :regions.region_name region) (= 1 1))
          (if-let [place (params "place")]
            (= :liftnew.place place) (= 1 1))
          (if-let [brigade (params "brigade")]
            (= :brigades.brigade brigade) (= 1 1))
          (if-let [building (params "building")]
            (= :build_types.building_name building) (= 1 1))
          (if-let [belong (params "belong")]
            (= :liftnew.belong belong) (= 1 1))
          (if-let [contract (params "contract")]
            (= :contracts.num_contract contract) (= 1 1))
          (if-let [maker (params "maker")]
            (= :liftmakers.maker_name maker) (= 1 1))
          (if-let [lifttype (params "lifttype")]
            (= :techspec.lifttype lifttype) (= 1 1))
          (if-let [speed (params "speed")]
            (= :liftmakers.speed speed) (= 1 1))

          (if-let [madefrom (params "made_year_from")]
            (>= :techspec.date_made (Integer/parseInt madefrom)) (= 1 1))

          (if-let [madeto (params "made_year_to")]
            (<= :techspec.date_made (Integer/parseInt madeto)) (= 1 1))

          (if-let [expfrom (params "exp_year_from")]
            (>= :techspec.date_made (Integer/parseInt expfrom)) (= 1 1))

          (if-let [expto (params "exp_year_to")]
            (<= :techspec.date_made (Integer/parseInt expto)) (= 1 1))
          
          (if-let [floor (params "floor")]
            (= :liftnew.floors floor) (= 1 1))
          (if-let [stop (params "stop")]
            (= :liftnew.stops stop) (= 1 1))
          (if-let [weight (params "weight")]
            (= :liftnew.weight weight) (= 1 1))
          (if-let [dtype (params "door_type")]
            (= :techspec.doortype dtype) (= 1 1))
          (if-let [mp_loc (params "mp_location")]
            (= :techspec.mp_location mp_loc) (= 1 1))
          (if-let [mp_enter (params "mp_enter")]
            (= :techspec.mp_enter mp_enter) (= 1 1))
          (if-let [fence (params "fence")]
            (= :techspec.cab_fense fence) (= 1 1))
          (if-let [mine (params "mine")]
            (= :techspec.mine mine) (= 1 1))
          (if-let [link (params "link")]
            (= :techspec.link link) (= 1 1))
          (if-let [ltype (params "link_type")]
            (= :techspec.link_pult_type ltype) (= 1 1))
          (if-let [lpult (params "link_pult")]
            (= :techspec.link_pult lpult) (= 1 1))
          (if-let [comcir (params "comcir")]
            (= :techspec.comcir comcir) (= 1 1))
          (if-let [circuit (params "circuit")]
            (= :techspec.circuit circuit) (= 1 1))
          (if-let [panel (params "panel")]
            (= :techspec.panel panel) (= 1 1))
          (if-let [reductor (params "reductor")]
            (= :techspec.reductor reductor) (= 1 1))
          (if-let [transfer (params "transfer")]
            (= :techspec.transfer_num transfer) (= 1 1))
          (if-let [set (params "set")]
            (= :techspec.set_comp set) (= 1 1))
          (if-let [bdiam (params "blockdiam")]
            (= :techspec.blockdiam bdiam) (= 1 1))
          (if-let [engine (params "engine")]
            (= :techspec.engine engine) (= 1 1))
          (if-let [e-form (params "engine_form")]
            (= :techspec.engine_form e-form) (= 1 1))
          (if-let [doorengine (params "door_engine")]
            (= :techspec.door_engine doorengine) (= 1 1))
          (if-let [de-form (params "door_engine_form")]
            (= :techspec.door_engine_form de-form) (= 1 1))
          (if-let [kvdiam (params "kvdiam")]
            (= :techspec.kvdiam kvdiam) (= 1 1))
          (if-let [kvnum (params "kvnum")]
            (= :techspec.kvnum kvnum) (= 1 1))
          (if-let [r-diam (params "rope_diam")]
            (= :techspec.rope_diam r-diam) (= 1 1))
          (if-let [r-len (params "rope_len")]
            (= :techspec.rope_len r-len) (= 1 1))
          (if-let [break (params "break_magnet")]
            (= :techspec.break_magnet break) (= 1 1))
          (if-let [fcons (params "floor_construct")]
            (= :techspec.construct fcons) (= 1 1))
          (if-let [ctrl (params "control")]
            (= :techspec.load_control ctrl) (= 1 1))
          (if-let [block (params "block")]
            (= :techspec.block block) (= 1 1))
          (if-let [balance (params "balance")]
            (= :techspec.balance balance) (= 1 1))
          
          (if-let [expertise-from (params "expertise_from")]
            (str "to_char (liftnew.last_exp_date,'YYYY') >= '" expertise-from "'") (= 1 1))

          (if-let [expertise-from (params "expertise_to")]
            (str "to_char (liftnew.last_exp_date,'YYYY') <= '" expertise-from "'") (= 1 1))

		  (if-let [con-beg-from (params "contract_begin_from")]
			(str "contracts.contract_begin >= '" con-beg-from "'") (= 1 1))

		  (if-let [con-beg-to (params "contract_begin_to")]
			(str "contracts.contract_begin <= '" con-beg-to "'") (= 1 1))

   		  (if-let [con-end-from (params "contract_end_from")]
			(str "contracts.contract_end >= '" con-end-from "'") (= 1 1))

		  (if-let [con-end-to (params "contract_end_to")]
			(str "contracts.contract_end <= '" con-end-to "'") (= 1 1))

		  )))
                  
       (cql/project [:liftnew.reg_num
					 :liftnew.weight
					 :contracts.contract_end])))

;;;
(defn db-get-streets
  []
  @(-> (cql/table *db* :streets)
       (cql/sort [:streets.street_name])
       (cql/distinct)))

(defn db-get-houses
  []
  @(-> (cql/table *db* :liftnew)
       (cql/project [:house])
       (cql/sort [:house])
       (cql/distinct)))

(defn db-get-holders
  []
  @(-> (cql/table *db* :contracts)
       (cql/project [:holder])
       (cql/sort [:holder])
       (cql/distinct)))

(defn db-get-belongs
  []
  @(-> (cql/table *db* :liftnew)
       (cql/project [:belong])
       (cql/sort [:belong])
       (cql/distinct)))

(defn db-get-places
  []
  @(-> (cql/table *db* :liftnew)
       (cql/project [:place])
       (cql/sort [:place])
       (cql/distinct)))

(defn db-get-buildings
  []
  @(-> (cql/table *db* :build_types)
       (cql/project [:building_name])
       (cql/sort [:building_name])
       (cql/distinct)))

(defn db-get-makers
  []
  @(-> (cql/table *db* :liftmakers)
       (cql/project [:maker_name])
       (cql/sort [:maker_name])
       (cql/distinct)))

(defn db-get-lifttypes
  []
  @(-> (cql/table *db* :techspec)
       (cql/project [:lifttype])
       (cql/sort [:lifttype])
       (cql/distinct)))

(defn db-get-speeds
  []
  @(-> (cql/table *db* :liftnew)
       (cql/project [:speed])
       (cql/sort [:speed])
       (cql/distinct)))

(defn db-get-weights
  []
  @(-> (cql/table *db* :liftnew)
       (cql/project [:weight])
       (cql/sort [:weight])
       (cql/distinct)))

(defn db-get-fences
  []
  @(-> (cql/table *db* :techspec)
       (cql/project [:cab_fense])
       (cql/sort [:cab_fense])
       (cql/distinct)))

(defn db-get-doortypes
  []
  @(-> (cql/table *db* :techspec)
       (cql/project [:door_type])
       (cql/sort [:door_type])
       (cql/distinct)))

(defn db-get-mines []
  @(-> (cql/table *db* :techspec)
       (cql/project [:mine])
       (cql/sort [:mine])
       (cql/distinct)))

(defn db-get-mp-places []
  @(-> (cql/table *db* :techspec)
       (cql/project [:mp_location])
       (cql/sort [:mp_location])
       (cql/distinct)))

(defn db-get-mp-enters []
  @(-> (cql/table *db* :techspec)
       (cql/project [:mp_enter])
       (cql/sort [:mp_enter])
       (cql/distinct)))

(defn db-get-links []
  @(-> (cql/table *db* :techspec)
       (cql/project [:link])
       (cql/sort [:link])
       (cql/distinct)))

(defn db-get-linktypes []
  @(-> (cql/table *db* :techspec)
       (cql/project [:link_pult_type])
       (cql/sort [:link_pult_type])
       (cql/distinct)))

(defn db-get-linkpults []
  @(-> (cql/table *db* :techspec)
       (cql/project [:link_pult])
       (cql/sort [:link_pult])
       (cql/distinct)))

(defn db-get-panels []
  @(-> (cql/table *db* :techspec)
       (cql/project [:panel])
       (cql/sort [:panel])
       (cql/distinct)))

(defn db-get-comcirs []
  @(-> (cql/table *db* :techspec)
       (cql/project [:comcir])
       (cql/sort [:comcir])
       (cql/distinct)))

(defn db-get-circuits []
  @(-> (cql/table *db* :techspec)
       (cql/project [:circuit])
       (cql/sort [:circuit])
       (cql/distinct)))

(defn db-get-reductors []
  @(-> (cql/table *db* :techspec)
       (cql/project [:reductor])
       (cql/sort [:reductor])
       (cql/distinct)))

(defn db-get-transfers []
  @(-> (cql/table *db* :techspec)
       (cql/project [:transfer_num])
       (cql/sort [:transfer_num])
       (cql/distinct)))

(defn db-get-sets []
  @(-> (cql/table *db* :techspec)
       (cql/project [:set_comp])
       (cql/sort [:set_comp])
       (cql/distinct)))

(defn db-get-engines []
  @(-> (cql/table *db* :techspec)
       (cql/project [:engine])
       (cql/sort [:engine])
       (cql/distinct)))

(defn db-get-eforms []
  @(-> (cql/table *db* :techspec)
       (cql/project [:engine_form])
       (cql/sort [:engine_form])
       (cql/distinct)))

(defn db-get-doorengines []
  @(-> (cql/table *db* :techspec)
       (cql/project [:door_engine])
       (cql/sort [:door_engine])
       (cql/distinct)))

(defn db-get-deforms []
  @(-> (cql/table *db* :techspec)
       (cql/project [:door_engine_form])
       (cql/sort [:door_engine_form])
       (cql/distinct)))

(defn db-get-kvdiams []
  @(-> (cql/table *db* :techspec)
       (cql/project [:kvdiam])
       (cql/sort [:kvdiam])
       (cql/distinct)))

(defn db-get-kvnums []
  @(-> (cql/table *db* :techspec)
       (cql/project [:kvnum])
       (cql/sort [:kvnum])
       (cql/distinct)))

(defn db-get-blockdiams []
  @(-> (cql/table *db* :techspec)
       (cql/project [:blockdiam])
       (cql/sort [:blockdiam])
       (cql/distinct)))

(defn db-get-ropediams []
  @(-> (cql/table *db* :techspec)
       (cql/project [:rope_diam])
       (cql/sort [:rope_diam])
       (cql/distinct)))

(defn db-get-ropelens []
  @(-> (cql/table *db* :techspec)
       (cql/project [:rope_len])
       (cql/sort [:rope_len])
       (cql/distinct)))

(defn db-get-breaks []
  @(-> (cql/table *db* :techspec)
       (cql/project [:break_magnet])
       (cql/sort [:break_magnet])
       (cql/distinct)))

(defn db-get-blocks []
  @(-> (cql/table *db* :techspec)
       (cql/project [:block])
       (cql/sort [:block])
       (cql/distinct)))

(defn db-get-constructs []
  @(-> (cql/table *db* :techspec)
       (cql/project [:construct])
       (cql/sort [:construct])
       (cql/distinct)))

(defn db-get-controls []
  @(-> (cql/table *db* :techspec)
       (cql/project [:load_control])
       (cql/sort [:load_control])
       (cql/distinct)))

(defn db-get-balances []
  @(-> (cql/table *db* :techspec)
       (cql/project [:balance])
       (cql/sort [:balance])
       (cql/distinct)))

(defn update-table
  [table key id datamap]
  (sql/with-connection *db*
	(sql/transaction
	 (sql/update-values
	  table
	  [(str key "=?") id]
	  datamap))))

(defn date-to-sql
  [value]
  (let [date (map #(Integer/parseInt %)
				  (.split value "-"))]
	(java.sql.Date. (- (first date) 1900)
					(- (second date) 1)
					(nth date 2))))

(defn db-update-maindata
  [params]
  (let [liftnew-datamap
		  (merge
		   (if-let [street (params "street")]
			 (hash-map :street_id
						(:id_street
						 (first
						  @(cql/select
							(cql/table *db* :streets)
							(cql/where (= :street_name street)))))))
		   (if-let [build-type (params "build_type")]
			 (hash-map :building_type
					   (:id_building
						(first
						 @(cql/select
						   (cql/table *db* :build_types)
						   (cql/where (= :building_name build-type)))))))
		   (if-let [val (params "corp")]
			 (hash-map :corp val))
		   (if-let [val (params "place")]
			 (hash-map :place val))
		   (if-let [val (params "inspection")]
			 (hash-map :inspection (date-to-sql val)))
		   (if-let [val (params "inspector")]
			 (hash-map :inspector val))
		   (if-let [val (params "forbidden")]
			 (hash-map :forbidden (if (= "true" val) true false)))
		   (if-let [val (params "forbid_date")]
			 (hash-map :forbid_date (date-to-sql val)))
		   (if-let [val (params "forbid_reason")]
			 (hash-map :forbid_reason val))
		   (if-let [speeds (params "speed")]
			 (hash-map :speed speeds))
		   (if-let [stops (params "stops")]
			 (hash-map :stops (Integer/parseInt stops)))
		   (if-let [comment (params "comment")]
			 (hash-map :comment comment)))]

	  (update-table :liftnew "reg_num" (params "reg_num") liftnew-datamap))

	(let [contracts-map
			 (merge
			  (if-let [val (params "holder")]
				(hash-map :holder val)))
			 id (:contract_id
				 (first @(cql/select
						  (cql/table *db* :liftnew)
						  (cql/where (= :reg_num (params "reg_num"))))))]
	  
	  (update-table :contracts "id_contract" id contracts-map)))
	   
;	  @(-> (cql/table *db* :liftnew)
;		   (cql/update-in!
;			(cql/where (= :reg_num (params "reg_num")))
;			liftnew-datamap))))


(defn db-update-techdata
  [params]
	(let [liftnew-techmap
		  (merge
		   (if-let [maker (params "maker")]
			 (hash-map :maker maker))
		   (if-let [ltype (params "lift_type")]
			 (hash-map :lifttype ltype))
		   (if-let [dtype (params "door_type")]
			 (hash-map :door_type dtype))
		   (if-let [value (params "mine")]
			 (hash-map :mine value))
		   (if-let [mp-enter (params "mp_enter")]
			 (hash-map :mp_enter mp-enter))
		   (if-let [mp-loc (params "mp_location")]
			 (hash-map :mp_location mp-loc))
		   (if-let [value (params "cab_fence")]
			 (hash-map :cab_fense value))
		   (if-let [value (params "link")]
			 (hash-map :link value))
		   (if-let [value (params "link_type")]
			 (hash-map :link_type value))
		   (if-let [value (params "link_pult")]
			 (hash-map :link_pult_type value))
		   (if-let [value (params "com_cir")]
			 (hash-map :comcir value))
		   (if-let [value (params "circuit")]
			 (hash-map :circuit value))
		   (if-let [value (params "panel")]
			 (hash-map :panel value))
		   (if-let [value (params "reductor")]
			 (hash-map :reductor value))
		   (if-let [value (params "transfer_num")]
			 (hash-map :transfer_num value))
		   (if-let [value (params "set_comp")]
			 (hash-map :set_comp value))
		   (if-let [value (params "blockdiam")]
			 (hash-map :blockdiam value))
		   (if-let [value (params "engine")]
			 (hash-map :engine value))
		   (if-let [value (params "engine_changed_date")]
			 (hash-map :engine_changed_date (date-to-sql value)))
		   (if-let [value (params "engine_changed_contract")]
			 (hash-map :engine_changed_contract value))
		   (if-let [value (params "engine_form")]
			 (hash-map :engine_form value))
		   (if-let [value (params "door_engine")]
			 (hash-map :door_engine value))
		   (if-let [value (params "doorengine_changed_date")]
			 (hash-map :doorengine_changed_date (date-to-sql value)))
		   (if-let [value (params "doorengine_changed_contract")]
			 (hash-map :doorengine_changed_contract value))
		   (if-let [value (params "door_engine_form")]
			 (hash-map :door_engine_form value))
		   (if-let [value (params "kvdiam")]
			 (hash-map :kvdiam value))
		   (if-let [value (params "kvnum")]
			 (hash-map :kvnum value))
		   (if-let [value (params "ropediam")]
			 (hash-map :rope_diam (Float/parseFloat value)))
		   (if-let [value (params "ropelen")]
			 (hash-map :rope_len (Float/parseFloat value)))
		   (if-let [value (params "tel_magnet")]
			 (hash-map :break_magnet value))
		   (if-let [value (params "floor_construct")]
			 (hash-map :floor_type value))
		   (if-let [value (params "balance")]
			 (hash-map :balance value))
		   (if-let [value (params "load_control")]
			 (hash-map :load_control value))
		   (if-let [value (params "block")]
			 (hash-map :block value))
		   (if-let [value (params "factory_num")]
			 (hash-map :factory_num value))
		   (if-let [value (params "ubl_type")]
			 (hash-map :ubl_type value))
		   (if-let [value (params "ubl_date")]
			 (hash-map :ubl_date (date-to-sql value)))
		   (if-let [value (params "shield")]
			 (hash-map :safety_shield (if (= "true" value) true false)))
		   )]
	  (println (str "....> " liftnew-techmap))

	  (sql/with-connection *db*
		(sql/transaction
		 (sql/update-values
		  :techspec
		  ["reg_num=?" (params "reg_num")]
		  liftnew-techmap)))))

(defn db-update-contractdata
  [params]
  (let [contracts-map
		 (merge
		  (if-let [val (params "contract")]
			(hash-map :num_contract val))
		  (if-let [val (params "holder")]
			(hash-map :holder val))
		  (if-let [val (params "holder_director")]
			(hash-map :holder_director val))
		  (if-let [val (params "holder_addr")]
			(hash-map :holder_addr val))
		  (if-let [val (params "holder_director_phone")]
			(hash-map :holder_director_phone val))
		  (if-let [val (params "contract_period")]
			(hash-map :contract_period val))
		  (if-let [val (params "contract_date")]
			(hash-map :contract_date (date-to-sql val)))
		  (if-let [val (params "contract_begin")]
			(hash-map :contract_begin (date-to-sql val)))
		  (if-let [val (params "contract_end")]
			(hash-map :contract_end (date-to-sql val)))
		  (if-let [val (params "podryad_usluga")]
			(hash-map :podryad_usluga (if (= "true" val) true false)))
		  (if-let [val (params "eto")]
			(hash-map :eto val))
		  (if-let [val (params "prolongation")]
			(hash-map :prolongation val))
		  (if-let [val (params "osvid_cost")]
			(hash-map :osvid_cost (Float/parseFloat val)))
		  (if-let [val (params "change_cost")]
			(hash-map :change_cost (if (= "true" val) true false)))
		  (if-let [val (params "stopandplombed_mp")]
			(hash-map :stopandplombed_mp (if (= "true" val) true false)))
		  (if-let [val (params "stopped")]
			(hash-map :stopped (if (= "true" val) true false)))
		  )]
	  
  (update-table :contracts "num_contract" (params "contract") contracts-map)))

(defn db-update-brigadedata
  [params]
  (let [brigade-map
		 (merge
		  (if-let [val (params "brigadir")]
			(hash-map :brigadir val))
		  (if-let [val (params "brigadir_addr")]
			(hash-map :b_addr val))
		  (if-let [val (params "brigadir_phone")]
			(hash-map :b_phone val))
		  (if-let [val (params "master")]
			(hash-map :master val))
		  (if-let [val (params "master_addr")]
			(hash-map :m_addr val))
		  (if-let [val (params "master_phone")]
			(hash-map :m_phone val))
;		  (if-let [val (params "nach_uchastka")]
;			(hash-map :next_exp_date (date-to-sql val)))
		  )]
	  
  (update-table :brigades "brigade" (params "brigade") brigade-map)))

(defn db-update-expertisedata
  [params]
  (let [exp-map
		 (merge
		  (if-let [val (params "last_exp_num")]
			(hash-map :last_exp_num val))
		  (if-let [val (params "exp_maker")]
			(hash-map :exp_maker val))
		  (if-let [val (params "exp_maker_addr")]
			(hash-map :exp_maker_addr val))
		  (if-let [val (params "exp_maker_phone")]
			(hash-map :exp_maker_phone val))
		  (if-let [val (params "exp_story")]
			(hash-map :exp_story val))
		  (if-let [val (params "last_exp_date")]
			(hash-map :last_exp_date (date-to-sql val)))
		  (if-let [val (params "next_exp_date")]
			(hash-map :next_exp_date (date-to-sql val)))
		  )])
  (update-table :liftnew "reg_num" (params "reg_num") exp-map))


(defn db-insert-newval
  [params]
  (if-let [mapdata
		(merge
		 (if-let [val (params "street")]
		   (hash-map :table :streets :street_name val))
		 ;(if-let [val (params "holder")]
		 ;  (hash-map :table :contracts :holder val)

		 )]
	(cql/conj!
	 (cql/table *db* (:table mapdata))
	 (dissoc mapdata :table))))

