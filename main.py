import redis, requests, json, sched, time, datetime, os, re
from dotenv import load_dotenv

load_dotenv()
s = sched.scheduler(time.time, time.sleep)


def useRegex(input):
    pattern = re.compile(r"[0-9]+[a-zA-Z]", re.IGNORECASE)
    return pattern.match(input)


def actualiser(sc):
    print("actualise - "+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    IDSAE = []
    path = os.getenv("URL")

    #Lecture des IDSAE dans REDIS
    r = redis.Redis(host=os.getenv("REDIS_HOST"), port=os.getenv("REDIS_PORT"), password=os.getenv("REDIS_PASS"))
    for idsae in r.keys():
        IDSAE.append(idsae.decode("utf-8"))
        path = path + "MonitoringRef="+idsae.decode("utf-8")+"&"
    path = path[:-1]


    #Requete OpenData
    req = requests.get(path, auth=(os.getenv("TOKEN"), ''))

    #Nettoyage des données
    data = req.json()['ServiceDelivery']['StopMonitoringDelivery'][0]['MonitoredStopVisit']
    my_data = {}


    for passage in data:
        idsae = passage['MonitoringRef']

        if useRegex(idsae):
            idsae = idsae[:-1]

        if not idsae in my_data:
            my_data[idsae]={
                "passages":[],
            }

        ligne = passage['MonitoredVehicleJourney']['PublishedLineName']
        destination = passage['MonitoredVehicleJourney']['DestinationName']
        heure = passage['MonitoredVehicleJourney']['MonitoredCall']['ExpectedDepartureTime']
        timestamp = time.mktime(datetime.datetime.strptime(heure.split('+')[0], "%Y-%m-%dT%H:%M:%S").timetuple())
        mytime = datetime.datetime.strptime(heure.split('+')[0], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y %H:%M:%S")

        my_data[idsae]['passages'].append({
            "ligne":ligne,
            "destination":destination,
            "heure":mytime
        })


    #Ecriture des données sur REDIS
    for idsae in r.keys():
        my_idsae = idsae.decode("utf-8")
        if my_idsae in my_data:
            r.set(my_idsae,json.dumps(my_data[my_idsae]))
        else:
            r.set(idsae,'')

    sc.enter(int(os.getenv("UPDATE_INTERVAL")), 1, actualiser, (sc,))



s.enter(1, 1, actualiser, (s,))
s.run()