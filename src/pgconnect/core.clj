(ns pgconnect.core
  (:require [clojureql.core :as cql]))

(def *db* {:classname "org.postgresql.Driver"
           :subprotocol "postgresql"
           :subname "//localhost/radel"
           :user "postgres"
           :password "123"})

(def regs ["Адмиралтейский" "Василеостровский" "Выборгский" "Калининский" "Кировский" "Колпинский" "Красногвардейский" "Красносельский" "Кронштадтский" "Курортный" "Ломоносовский" "Московский" "Невский" "Павловский" "Петроградский" "Петродворцовый" "Приморский" "Пушкинский" "Фрунзенский" "Центральный"])

(defn insert-into-table-example []
  (time (doseq [b bb]
          (cql/conj! (cql/table *db* :build_types) {:building_name (:building b)}))))


(def reg-ids @(cql/table *db* :regions))
(def brig-ids @(cql/table *db* :brigades))
(def street-ids @(cql/table *db* :streets))
(def maker-ids @(cql/table *db* :liftmakers))
(def building-ids @(cql/table *db* :build_types))
(def contract-ids @(cql/table *db* :contracts))

(defn t-move-data []
  (doseq [record (deref
           (-> (cql/table *db* :lift)
               (cql/project [:regnum
                             :street
                             :house
                             :corp
                             :parnum
                             :region
                             :place
                             :brigade
                             :deu
                             :zsk
                             :dexp
                             :nextexp
                             :exps
                             :floor
                             :stop
                             :speed
                             :maker
                             :building
                             :contract
                             :on_serve
                             :date
                             :exploat])
               ;(cql/take 2000)
               ))]
    (let [region-id (:id_region (first
                          (filter #(= (:region_name %) (:region record)) reg-ids)))
          brig-id (:id_brigade
                   (first
                    (filter #(= (:brigade %) (:brigade record)) brig-ids)))
          street-id (:id_street
                     (first
                      (filter #(= (:street_name %) (:street record))
                              street-ids)))
          building-id (:id_building
                     (first
                      (filter #(= (:building_name %) (:building record))
                              building-ids)))
          contract-id (:id_contract
                     (first
                      (filter #(= (:num_contract %) (:contract record))
                                                           contract-ids)))
          ]
      (print (str (:regnum record) " | "))
      (cql/conj! (cql/table *db* :liftnew)
                 {:region region-id
                  :reg_num (:regnum record)
                  :street_id street-id ;(:street record)
                  :house (:house record)
                  :corp (:corp record)
                  :parnum (:parnum record)
                  :deu (:deu record)
                  :zsk (:zsk record)
                  :place (:place record)
                  :brigade_id (if (nil? brig-id) 0 brig-id)
                  :last_exp_date (:dexp record)
                  :next_exp_date (:nextexp record)
                  :exp_story (:exps record)
                  :floors (:floor record)
                  :stops (:stop record)
                  :speed (:speed record)
                  :building_type building-id
                  :contract_id (if (nil? contract-id) 0 contract-id)
                  :is_served (:on_serve record)
                  :belong (:belong lift)
                  :weight (:weight lift)}))))

(defn specs-move-data []
  (doseq [record @(-> (cql/table *db* :liftnew)
                      (cql/project [:reg_num]))]
    (let [lift (first @(cql/select (cql/table *db* :lift) (cql/where (= (:reg_num record) :regnum))))
          maker-id (:id_maker
                    (first (filter #(= (:maker_name %)
                                       (:maker lift))
                                   maker-ids)))]
      (print (str (:reg_num record) " | "))
    (cql/conj! (cql/table *db* :techspec)
               {:reg_num (:regnum lift)
                :maker maker-id ; (:maker lift)
                :maker_id maker-id
                :door_type (:door lift)
                :mine (:mine lift)
                :mine_size (:sh_size lift)
                :mine_height (:height lift)
                :cab_size (:cab_size lift)
                :cab_fense (:fense lift)
                :mp_enter (:entrance lift)
                :mp_location (:location lift)
                :date_expl (:exploat lift)
                :date_made (:date lift)
                :factory_num (:number lift)
                :link (:ink lift)
                :link_pult (:control lift)
                :link_pult_type (:linktype lift)
                :balance (:balance lift)
                :comcir (:comcir lift)
                :circuit (:circuit lift)
                :block (:block lift)
                :reductor (:reductor lift)
                :transfer_num (:transfer lift)
                :floor_type (:construct lift)
                :engine (:engine lift)
                :door_engine (:drive lift)
                :panel (:panel lift)
                :engine_form (:enginefo lift)
                :door_engine_form (:doorsfor lift)
                :kvdiam (:kvdiam lift)
                :kvnum (:kvnum lift)
                :blockdiam (:blockdiam lift)
                :construct (:construct lift)
                :rope_diam (:ropediam lift)
                :rope_len (:ropelen lift)
                :break_magnet (:breaks lift)
                :load_control (:loadcount lift)
                :set_comp (:set_ lift)}))))

(defn contract-move-data []
  (doseq [record @(-> (cql/table *db* :liftnew)
                      (cql/project [:reg_num]))]
    (let [c (first @(-> (cql/select (cql/table *db* :lift) (cql/where (= (:reg_num record) :regnum))) cql/distinct))]
      (if (empty? @(cql/select (cql/table *db* :contracts)
                               (cql/where (= :num_contract
                                             (:contract c)))))
        (cql/conj! (cql/table *db* :contracts)
                   {:num_contract (:contract c)
                    :holder (:holder c)
                    :contract_begin (:dbegin c)
                    :contract_end (:dend c)
                    :reason (:obosn c)
                    :area (:s2 c)
                    :cost_osvid (:servecost c)
                    :prolongation (str "")
                    :podryad_usluga true
                    :eto true
                    :change_cost true
                    :stopandplombed_mp true
                    :supervisor (str "")
                    :s_addr (str "")
                    :s_telfax (str "")})))))


;; UPDATE contracts SET prolongation=imp_uk.prolongation FROM imp_uk WHERE num_contract=imp_uk.num_dog

;; UPDATE contracts SET contract_end=imp_uk.end FROM imp_uk WHERE num_contract=imp_uk.num_dog

;; UPDATE contracts SET contract_date=imp_uk.date_podpis FROM imp_uk WHERE num_contract=imp_uk.num_dog

;; UPDATE contracts SET contract_period=imp_uk.srok FROM imp_uk WHERE num_contract=imp_uk.num_dog

;; UPDATE contracts SET solution=imp_uk.solution FROM imp_uk WHERE num_contract=imp_uk.num_dog

;; UPDATE contracts SET priostanovleno=true FROM imp_uk WHERE num_contract=imp_uk.num_dog AND imp_uk.priost='����'

;; UPDATE contracts SET stopandplombed_mp=false FROM imp_uk WHERE num_contract=imp_uk.num_dog AND imp_uk.priost<>'�������� ��'

;; UPDATE contracts SET change_cost=false FROM imp_uk WHERE num_contract=imp_uk.num_dog AND imp_uk.changed<>'����'

;; UPDATE contracts SET eto=imp_uk.eto FROM imp_uk WHERE num_contract=imp_uk.num_dog

;; 
