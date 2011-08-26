(ns pgconnect.server
  (:use compojure.core)
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:use [ring.middleware file file-info params])
  (:require [pgconnect.logic :as logic])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]))

(defn header []
  (str "HTTP/1.1 200 OK\n"
       "Content-Type: text/plain; charset=utf-8\n\n"))

(defroutes main-routes
  (GET "/" [] (str "***  E11: bad request: Radel http server"))

  (POST "/regnum" {params :params}
        (header)
        (logic/get-regnumdata (params "regnum")))

  (POST "/regnums" {params :params}
        (header)
        (logic/get-sel-regnums (params "regnum")))

  (POST "/sel_addrs" {params :params}
        (header)
        (logic/get-sel-addrs (params "addrmask")))

  (POST "/brigade" {params :params}
        (header)
        (logic/get-brigade-data (params "brigade")))

  (POST "/brigadework" {params :params}
        (header)
        (logic/get-brigadework (params "brigade")))

  (POST "/contractwork" {params :params}
        (header)
        (logic/get-contractwork (params "contract")))
  
  (POST "/specs" {params :params}
        (header)
        (logic/get-sel-spec (params "regnum")))

  (POST "/contract" {params :params}
        (header)
        (logic/get-sel-contract (params "contract")))
  
  (POST "/expertise" {params :params}
        (header)
        (logic/get-sel-expertise (params "regnum")))

  (POST "/region" {params :params}
        (header)
        (logic/get-sel-region (params "region")))

  (GET "/s_regnums" [] (logic/get-regnums))
  (GET "/s_addrs" [] (logic/get-addresses))
  (GET "/s_regions" [] (logic/get-regions))
  (GET "/s_brigades" [] (logic/get-brigades))
  (GET "/s_contracts" [] (logic/get-contracts))

  (GET "/s_streets" [] (logic/get-streets))
  (GET "/s_houses" [] (logic/get-houses))
  (GET "/s_holders" [] (logic/get-holders))
  (GET "/s_belongs" [] (logic/get-belongs))
  (GET "/s_places" [] (logic/get-places))
  (GET "/s_buildings" [] (logic/get-buildings))
  (GET "/s_makers" [] (logic/get-makers))
  (GET "/s_lifttypes" [] (logic/get-lifttypes))
  (GET "/s_speeds" [] (logic/get-speeds))
  (GET "/s_weights" [] (logic/get-weights))
  (GET "/s_doortypes" [] (logic/get-doortypes))
  (GET "/s_fences" [] (logic/get-fences))
  (GET "/s_mines" [] (logic/get-mines))
  (GET "/s_mp_places" [] (logic/get-mp-places))
  (GET "/s_mp_enters" [] (logic/get-mp-enters))
  (GET "/s_links" [] (logic/get-links))
  (GET "/s_linktypes" [] (logic/get-linktypes))
  (GET "/s_linkpults" [] (logic/get-linkpults))
  (GET "/s_panels" [] (logic/get-panels))
  (GET "/s_comcirs" [] (logic/get-comcirs))
  (GET "/s_circuits" [] (logic/get-circuits))
  (GET "/s_reductors" [] (logic/get-reductors))
  (GET "/s_transfernums" [] (logic/get-transfers))
  (GET "/s_sets" [] (logic/get-sets))
  (GET "/s_engines" [] (logic/get-engines))
  (GET "/s_engineforms" [] (logic/get-e-forms))
  (GET "/s_doorengines" [] (logic/get-doorengines))
  (GET "/s_doorengineforms" [] (logic/get-de-forms))
  (GET "/s_kvdiams" [] (logic/get-kvdiams))
  (GET "/s_kvnums" [] (logic/get-kvnums))
  (GET "/s_blockdiams" [] (logic/get-blockdiams))
  (GET "/s_ropediams" [] (logic/get-ropediams))
  (GET "/s_ropelens" [] (logic/get-ropelens))
  (GET "/s_telmagnets" [] (logic/get-breaks))
  (GET "/s_blocks" [] (logic/get-blocks))
  (GET "/s_constructs" [] (logic/get-constructs))
  (GET "/s_controls" [] (logic/get-controls))
  (GET "/s_balances" [] (logic/get-balances))

  (POST "/get_fsdata" {params :params}
        (println (str "==> " params))
        (header)
        (logic/get-fullselect-data params))

  (POST "/update_maindata" {params :params}
		(println (str ":: to update ==>" params))
		(logic/update-maindata params))

  (POST "/update_techdata" {params :params}
        	(println (str ":: to tech update ==>" params))
        	(logic/update-techdata params))

  (POST "/update_contractdata" {params :params}
        	(println (str ":: contract update ==>" params))
        	(logic/update-contractdata params))

  (POST "/update_brigadedata" {params :params}
		(println (str ":: brigade update ==>" params))
		(logic/update-brigadedata params))
  
  (POST "/update_expertisedata" {params :params}
		(println (str ":: exp update ==>" params))
		(logic/update-expertisedata params))

  (POST "/insert_newval" {params :params}
		(println (str ":: to insert ==>" params "\n"))
		(logic/insert-newval params))

  (POST "/auth" {params :params}
		(logic/auth params))
    
  (route/files "/"))

(def app
  (-> #'main-routes
      wrap-params
      (wrap-file "public")
      (wrap-file-info)))

(defonce server
  (run-jetty
   app
   {:port 8000 :join? false}))

(defn -main [& args]
  (run-jetty app {:port 8080 :join? false}))
