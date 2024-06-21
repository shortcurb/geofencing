import datetime,json,pytz,os,requests,hashlib,time
from support_connections.database import Database
#from support_connections.apis import Hasura
from support_connections.record import RecordFetcher
from dotenv import load_dotenv
from haversine import haversine,Unit
from support_connections.apis import Slack
from support_connections.identifiers import Identify



load_dotenv()
db = Database()
rec = RecordFetcher()


def composemessage(veh,marketinfo,lr,mms):
    # figure out how to pull the license plate
    lred = datetime.datetime.strftime(lr.event_date.astimezone(pytz.timezone('America/Chicago')),'%m-%d-%y %I:%M%p %Z')
    gmapsurl = f"https://www.google.com/maps/place/{lr.coordinates_y},{lr.coordinates_x}"
    ismiurl = f"https://connect.fluidtruck.com/web/command/{veh['ismi']}" # formatting URL part of the message
    imeiurl = f"https://fm.teltonika.lt/devices?per_page=100&query={veh['imei']}&selected={veh['imei']}"
    iccidurl = f"https://simcontrolcenter.wireless.att.com/provision/ui/terminals/sims/sims.html?simsGrid-search=%5B%7B%22name%22%3A%22oneBox%22%2C%22display%22%3A%22{veh['iccid']}%22%2C%22value%22%3A%22{veh['iccid']}%22%2C%22type%22%3A%22CONTAINS%22%7D%5D"


    text = ":rotating_light: There is a vehicle within 50 miles of the US/Mexico border :rotating_light:\n*Vehicle Info*\n"
    text += f"FN: {veh['fleetnumber']}\nVIN: {veh['vin']}\n"
    text += f"Model: {str(veh['year'])}' '{veh['model']}\nMarket: {veh['market']}\n"
    text +="*Device Info*\n"
    text += f"ISMI: <{ismiurl}|{veh['ismi']}>\n"
    text += f"IMEI: <{imeiurl}|{veh['imei']}>\n"
    text += f"ICCID: <{iccidurl}|{veh['iccid']}>\n"
    text += "*Record Info*\n"
    text += f"Last Record: {lred}\nSpeed: {lr.speed}\nFuel: {lr.fuel_level}\n"
    text += f"Ignition: {lr.event_data_IoItems_Ignition}\nGSM Level: {lr.event_data_IoItems_GSM_level}\n"
    text += f"<{gmapsurl}|Location>\n"
    text += "*Tags*\n"
    for mm in mms:
        text += f"<@{mm['slackuserid']}>\n"
    

    rmmtag = marketinfo[0]['rmmemail'].split('@')[0]
    romtag = marketinfo[0]['romemail'].split('@')[0]
    region = marketinfo[0]['romrmmregion']

    gmdict = { # going to need to work on this, GM east/west doesn't jive anymore
        'GM East':'U01U4JE8NS0',
        'GM West':'U03Q9PMP2JZ',
        'GM Central':'U01BC1MT2PR'
    }

    try:
        text = text + f"<@{gmdict[region]}>\n"
    except KeyError:
        pass
    text = text + f"<@{romtag}>\n"
    text = text + f"<@{rmmtag}>\n"
#    print(text)
    return(text)

def findchannel(marketinfo):
    if os.environ['runtimecontext'] == 'stage':
        return('C05AQH4E39T')
    mktname = 'ops-'+marketinfo['name'].lower().replace(' ','')
    channelid = db.execute_query('SELECT channelid FROM slackchannels WHERE name = ?',[mktname])
    return(channelid)

def findsenders():
    then = datetime.datetime.now()-datetime.timedelta(hours = 1)
    sendable = db.execute_query("SELECT f.vin,v.ismi,v.market,v.fleetnumber,v.model,v.year,d.ismi,d.imei,d.iccid FROM fencedvehicles f INNER JOIN vehicles v ON v.vin = f.vin INNER JOIN devices d ON v.ismi = d.ismi WHERE lastalert < ? OR lastalert is Null",[then])
    for veh in sendable[:1]:


        marketinfo = db.execute_query("SELECT * FROM markets WHERE name = ?",[veh['market']])
        latest_record = rec.get_and_parse_latest_record(veh['ismi'])
        mms = db.execute_query('SELECT name,slackuserid FROM users WHERE markets =? AND slackuserid != "" AND active = True AND role LIKE "%Market Manager%"',[veh['market']])
#        print('mms',mms)
        text = composemessage(veh,marketinfo,latest_record,mms)
        channel = findchannel(marketinfo[0])
        slackpayload = {
            'url':'chat.postMessage',
            'data':{'channel':channel,'text':text,'unfurl_links':False}
        }
        print(slackpayload)
        
        Slack().req_slack(slackpayload)


findsenders()