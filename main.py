import json
import os
from decimal import Decimal
from time import sleep, time, gmtime, mktime
from selenium import webdriver
from selenium.webdriver import DesiredCapabilities
from selenium.webdriver.common.by import By
from mysql.connector import connect

myConnection = connect(
  host="89.221.222.9",
  user="nom",
  password="974S.nnim",
  database="zedrun"
)




def process_browser_logs_for_network_events(logs):
    webSoc  = []
    for entry in logs:
        log = json.loads(entry["message"])["message"]
        if "Network.webSocketFrameReceived" in log["method"]:
            webSoc.append(entry)
    return webSoc

def processDataPayLoad(logs, raceId):
    cursor = myConnection.cursor()
    cursor.execute('DELETE FROM raceDistanceDetail WHERE raceId="'+raceId+'"')
    cursor.execute('DELETE FROM racePosition WHERE raceId="'+raceId+'"')
    myConnection.commit()
    cursor = myConnection.cursor()
    sql = "INSERT INTO raceDistanceDetail (raceId, horseId, distance, timestamp) VALUES "
    positions = None
    start = None
    firstSoc = True
    stamp = 0
    previousDistances = None
    previousTimestamp = None
    distances = None
    for log in logs:
        socket = json.loads(log["message"])["message"]
        timeStamp = Decimal(str(socket['params']['timestamp']))

        if firstSoc:
            try:
                topData = json.loads(socket['params']['response']['payloadData'])
                race = topData[2].split("<")[0].split(":")[1]
                start = timeStamp
                if raceId == race:
                    firstSoc = False
                continue
            except:
                continue
        data = json.loads(socket['params']['response']['payloadData'])

        if data[3]=='updt_race_distance':
            if not previousTimestamp:
                previousTimestamp = start
            if start:
                print(timeStamp - previousTimestamp, end=" - ")
                print(timeStamp - start)
            stamp += 1
            previousDistances=distances
            previousTimestamp=timeStamp
            distances = data[4]['distances']
            for i in range(len(distances)):

                distance = distances[i]
                if previousDistances:
                    previousDistance = list(previousDistances[i].values())[0]
                else:
                    previousDistance = 0



                actualDistance = list(distance.values())[0] - previousDistance - 3.75

                horseID = list(distance.keys())[0]

                val = (raceId, horseID, round(actualDistance,4), Decimal(stamp)*Decimal('0.25'))


                if stamp==1 and i==0:
                    sql += ""
                else:
                    sql += ","

                sql += '("'+raceId+'",'+str(horseID)+',"'+ str(round(actualDistance,4)) +'","'+str(Decimal(stamp)*Decimal('0.25')) + '")'
        if data[3]=='updt_finish_positions':
            positions = data[4]['positions']
    cursor.execute(sql)

    myConnection.commit()
    cursor = myConnection.cursor()
    sqlP = "INSERT INTO racePosition (raceId, horseId, time, position) VALUES (%s, %s, %s, %s)"

    for i in range(len(positions)):
        positionG = positions[i]
        position = i + 1
        horseID = list(positionG.keys())[0]
        # gate = sPositions.index(positionG) + 1

        time = list(positionG.values())[0]
        val = (raceId, horseID, round(time, 4), position)
        cursor.execute(sqlP, val)
        condition = ' WHERE horseId='+horseID+' and timestamp >= "' + str(time) + '"'
        getAfterFinishDistance = 'SELECT distance FROM raceDistanceDetail' + condition
        cursor.execute(getAfterFinishDistance)
        result = cursor.fetchall()
        actualDistance = result[-1][0]
        cursor.execute("DELETE FROM raceDistanceDetail"+condition)
    myConnection.commit()

    setRaceStatus(3, raceId)

def current_milli_time():
    return round(time() * 1000)

def getUnprocessedRaces():
    cursor = myConnection.cursor()
    getRacesSQL = 'SELECT id FROM zedrun.race WHERE processed=0 ORDER BY country, city, name, distance, class DESC, start_time DESC'
    cursor.execute(getRacesSQL)
    result = cursor.fetchall()
    return result



def setRaceStatus(status, raceId):
    cursor = myConnection.cursor()
    sqlSetRaceProcesed = 'UPDATE race SET processed='+str(status)+' WHERE id="' + raceId + '"'
    cursor.execute(sqlSetRaceProcesed)
    myConnection.commit()


recording = False

workers = []
workersRow = 1
workersCol = 1
workerColSize = 1920/workersCol
workerRowSize = 1080/workersRow


def getNewWorker(colSize, rowSize, colPosition, rowPosition):
    global browser
    caps = DesiredCapabilities.CHROME
    caps['goog:loggingPrefs'] = {'performance': 'ALL'}
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

    chrome_options.binary_location = os.environ.get("GOOGLE_CHROME_BIN")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")

    browser = webdriver.Chrome(executable_path=os.environ.get("CHROMEDRIVER_PATH"), options=chrome_options, desired_capabilities=caps)
    browser.set_window_size(colSize, rowSize)
    browser.set_window_position(colPosition, rowPosition)
    return browser


for col in range(workersCol):
    for row in range(workersRow):
        browser = getNewWorker(workerColSize, workerRowSize, col * workerColSize, row * workerRowSize)
        workers.append([browser,"",workerColSize, workerRowSize, col*workerColSize,row*workerRowSize])

freeWorkers = []
loadedWorkers = []
activeWorkers = []
processingWorkers = []
movingWorkers = []
for worker in workers:
    freeWorkers.append(worker)

cursor = myConnection.cursor()
cursor.execute('UPDATE zedrun.race SET processed=0 WHERE processed = 1;')

horses = ["66994", "88764"]

for horseId in horses:
    races = getUnprocessedRaces()
    while len(races) > 0:
        try:
            for i in range(len(freeWorkers)):
                worker = freeWorkers[i]
                browser = worker[0]
                worker[1] = races[i][0]
                setRaceStatus(1, worker[1])

                browser.get("https://3d-racing.zed.run/replay/" + worker[1])
                movingWorkers.append(worker)
                print("add worker")

            for worker in movingWorkers:
                if worker in freeWorkers:
                    freeWorkers.remove(worker)
                    loadedWorkers.append(worker)
            movingWorkers = []

            for i in range(len(loadedWorkers)):
                worker = loadedWorkers[i]
                browser = worker[0]
                skip = browser.find_elements(By.XPATH, '//a[text()="Skip intro"]')
                horseRacing = browser.find_elements(By.ID, "horse-listing")
                raceEl = browser.find_elements(By.XPATH, '//a[text()="Watch again"]')
                start = gmtime()
                while len(skip) == 0 and len(horseRacing) == 0 and len(raceEl) == 0 and (mktime(gmtime()) - mktime(start)) < 10:
                    skip = browser.find_elements(By.XPATH, '//a[text()="Skip intro"]')
                    horseRacing = browser.find_elements(By.ID, "horse-listing")
                    raceEl = browser.find_elements(By.XPATH, '//a[text()="Watch again"]')
                if len(skip)!=0:
                    try:
                        if skip[0].is_displayed():
                            skip[0].click()
                    except:
                        pass
                if len(skip) != 0 or len(horseRacing) != 0 or len(raceEl) != 0:
                    movingWorkers.append(worker)
                else:
                    browser.refresh()
                print("loaded worker")

            for worker in movingWorkers:
                if worker in loadedWorkers:
                    loadedWorkers.remove(worker)
                    activeWorkers.append(worker)
            movingWorkers = []

            for i in range(len(activeWorkers)):
                worker = activeWorkers[i]
                browser = worker[0]
                raceEl = browser.find_elements(By.XPATH, '//a[text()="Watch again"]')
                if not (raceEl is None or len(raceEl) == 0):
                    movingWorkers.append(worker)
                    print("worker ended")

                print("active worker")

            for worker in movingWorkers:
                if worker in activeWorkers:
                    activeWorkers.remove(worker)
                    processingWorkers.append(worker)
            movingWorkers = []

            for i in range(len(processingWorkers)):
                worker = processingWorkers[i]
                browser = worker[0]
                logs = browser.get_log("performance")
                webSoc = process_browser_logs_for_network_events(logs)

                processDataPayLoad(webSoc, worker[1])
                movingWorkers.append(worker)

                print("process logs success")

            for worker in movingWorkers:
                if worker in processingWorkers:
                    processingWorkers.remove(worker)
                    freeWorkers.append(worker)
            movingWorkers = []

            if len(freeWorkers) != 0:
                races = getUnprocessedRaces()

        except Exception as e:
            if worker in freeWorkers:
                freeWorkers.remove(worker)

            if worker in loadedWorkers:
                loadedWorkers.remove(worker)

            if worker in activeWorkers:
                activeWorkers.remove(worker)

            if worker in processingWorkers:
                processingWorkers.remove(worker)

            if worker in movingWorkers:
                movingWorkers.remove(worker)

            browser = getNewWorker(worker[2], worker[3], worker[4], worker[5])
            worker[0] = browser
            freeWorkers.append(worker)