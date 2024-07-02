import mariadb,datetime,os,requests,json,inspect,smtplib,base64,io,zipfile,shutil,csv,psutil,asyncio,hashlib,re,pytz,functools,gspread
# import time # DO NOT IMPORT TIME, do not use time. Most of these functions are run with async. Using time.sleep instead of await asyncio.sleep defeats the purpose of asyncio entirely
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def trackcalls(func):
    # I can't quite get this to work. chatGPT says I can use this to create a list of all
    # The functions that are called. It returns some, but not everything. Need to do more investigating to understan this
    """Async-aware decorator to track function calls."""
    # Check if the function is a coroutine (async)
    if asyncio.iscoroutinefunction(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            wrapper.calls.append(func.__name__)
            return await func(*args, **kwargs)
    else:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            wrapper.calls.append(func.__name__)
            return func(*args, **kwargs)
    
    wrapper.calls = []
    return(wrapper)

@trackcalls
def simplezohohit(url,params):
    token = os.environ['zohotoken']
    headers = {'Authorization':f"Zoho-oauthtoken {token}"}
    r = requests.request("GET",url,headers=headers,params=params)
    return(r)

@trackcalls
def hitslack(method,url,payload):
    headers = {'Authorization': 'Bearer '+os.environ['slackapitoken'],'Content-Type': 'application/json'}
    r = requests.request(method,url,headers = headers,data = json.dumps(payload))
    return(r)

@trackcalls
def hitslacknojson(method,url,payload):
    headers = {'Authorization': 'Bearer '+os.environ['slackapitoken'],'Content-Type': 'application/x-www-form-urlencoded'}
    r = requests.request(method,url,headers = headers,params = payload)
    return(r)

@trackcalls
def hithasura(endpoint):
#    endpoint = 'fetchreliability' # Endpoint is appended to the hasuraurl
    headers = {
        'Content-Type':'application/json',
#        'Content-Type':'xxx-form-urlencoded',
        'Hasura-Client-Name':'hasura-console',
        'x-hasura-admin-secret':os.environ['hasurasecret']
    }
    url = os.environ['hasuraurl']+endpoint
    r = requests.request('GET',url, headers=headers)   
    return(r) 

@trackcalls
def uploadslack(method,url,payload,file):
    headers = {'Authorization': 'Bearer '+os.environ['slackapitoken']}
    with open(file,'rb') as f:
        files = {'file':('records.csv',f,'text/csv')}
        r = requests.request(method,url,headers = headers, data = payload, files=files)
    return(r)    

@trackcalls
async def getstats(waittime,stats):
    try:
        while True:
            memory = psutil.virtual_memory()
            cpu = psutil.cpu_percent(interval=None) 
            stats.append([datetime.datetime.now(),cpu,memory.percent])
            await asyncio.sleep(waittime)
    except asyncio.CancelledError:
        print('gettask cancelled (not a bad thing)')
    finally:
        print('gettask finished (not a bad thing)')

@trackcalls
async def runpull(pullfunc,alteredentity,entitytype,dependencies):
    # dependencies is a dictionary of processname:integer-seconds-since-update
    start = datetime.datetime.now()
    stats = []
    try:
        # This second checks for successfully-run dependencies and raises an error if any dependencies don't match
        for processname,secondssinceupdate in dependencies.items():
            datelimit = start-datetime.timedelta(seconds=secondssinceupdate)
            processq = executequeryoc('functiondb','SELECT * FROM functionstate WHERE successbool = 1 AND processname = %s AND lastranat >= %s',[processname,datelimit])
            if processq == []:
                output = f"Dependent process {processname} has not successfully run within the last {secondssinceupdate/60} minutes"
                raise ValueError(output)
            
        # This actually await runs the function
        if inspect.iscoroutinefunction(pullfunc):
            stattask = asyncio.create_task(getstats(1,stats))
            output = await pullfunc()
            stattask.cancel()
            if stats != []:
                writemanyoc('functiondb','INSERT INTO computingstate (datetime,cpuuse,memoryuse) VALUES (?,?,?) ON DUPLICATE KEY UPDATE datetime=datetime',stats)
        else:
            raise TypeError(f"{pullfunc.__name__} must be an async coroutine")
        successbool = True
        
    except Exception as e:
        output = str(e) # Make sure to stringify it, or you won't be able to insert it in the table
        print('Exception',e)
        successbool = False
    end = datetime.datetime.now()
    duration = (end-start).total_seconds()
    statequery = 'INSERT INTO functionstate (processname,alteredentity,entitytype,lastranat,runduration,successbool,output) VALUES (?,?,?,?,?,?,?) on duplicate key update processname=VALUES(processname),alteredentity=VALUES(alteredentity),entitytype=VALUES(entitytype),lastranat=VALUES(lastranat),runduration=VALUES(runduration),successbool=VALUES(successbool),output=VALUES(output)'
    statedata = [
        pullfunc.__name__,
        alteredentity,
        entitytype,
        start,
        duration,
        successbool,
        output,
    ]
    summaryquery = 'INSERT INTO functionsummary (functionname,starttime,endtime,duration,output,successbool,type) VALUES (?,?,?,?,?,?,?)'
    summarydata = [
        pullfunc.__name__,
        start,
        end,
        duration,
        output,
        successbool,
        entitytype
    ]
    conn = dbconn('functiondb')
    writequery(conn,statequery,statedata)
    writequery(conn,summaryquery,summarydata)
    conn.close()    

@trackcalls
def hitatt(urlsuffix,method,headers,payload,parambool):
    authstring = os.environ['attauthstring']
    authbytes = authstring.encode("ascii")
    auth64bytes = base64.b64encode(authbytes)
    auth64string = auth64bytes.decode("ascii")
    headers.update({
        "Accept": "application/json",
        "Authorization": f"Basic {auth64string}",
    }   )
    url = f"{os.environ['attbaseurl']}{urlsuffix}"
    if payload == False:
        r = requests.request(method,url,headers = headers)
    else:
        if parambool:
#            headers.update({"Content-Type":"x-www-form-urlencoded"}) # for getting SMS details, don't use any header. Who tf knew.
            r = requests.request(method,url,headers = headers,params=payload)
        else:
            headers.update({"Content-Type":"application/json"})
            r = requests.request(method,url,headers = headers,data=json.dumps(payload))
    return(r)

def sendemail(recipient_list, subject, body, file_path):
    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.login(os.getenv('workemailaddress'), os.getenv('gmailpw'))
    message = MIMEMultipart()
    message["From"] = os.getenv('workemailaddress')
    message["To"] = ", ".join(recipient_list)
    message["Subject"] = subject
    message.attach(MIMEText(body, 'plain'))
    # Attach the file
    part = MIMEBase('application', "octet-stream")
    if file_path != False:
        with open(file_path, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
        message.attach(part)

    # Send email to each recipient
    server.send_message(message)
    # Terminate the SMTP session and close the connection
    server.quit()

@trackcalls
async def asyncrequest(url,payload):
    # This funciton requsts a download of the records at the URL and returns the ID of the file to be downloaded later
    # Technically I can specify what fields to download, but I haven't tests it. Something with criteria ={}
    token = os.environ['zohotoken'] # Read zoho token
    waittime = 15
    qtychecks = 20
    headers = {'Authorization':f"Zoho-oauthtoken {token}",'Accept':'application/json','Content-Type':'application/json'}
    r = requests.request('POST',url,data = json.dumps(payload),headers=headers) # POSTing to url
    print('Creating async download request response:',r,r.text)
    id = r.json()['details']['id']
    for i in range(0,qtychecks):
        output = asynccheck(id,url) 
        if output != False and output != None:
            return(output)
        elif output == False:
            print('Download failed, Zoho error')
            return(False)
        else: # elif output == None
            print(f"Sleeping for {waittime} seconds, {qtychecks-i} checks remaining.")
            await asyncio.sleep(waittime)
        #    time.sleep(waittime)
    print('Failed download, exceeded checks')
    return(False)

@trackcalls
def zohofiledownload(url,id):
    outlist = []
    # This function hits a Zoho url, downloads, saves, and extracts the .zip file into a directory, reads the .csv file, then returns a list of its rows in a dictionary format
    token = os.environ['zohotoken'] # Reads zoho token from zohotokenfile
    headers = {'Authorization':f"Zoho-oauthtoken {token}"}
    r = requests.request('GET',url+'/'+id+'/result',headers = headers) # downloads file 
    print('Downloading file response:',r)
    z = zipfile.ZipFile(io.BytesIO(r.content)) # IDK something with the .zip file
    ziploc = os.environ['HOME']+'/'+id # zip file location (which is the ID returned by Zoho in asyncrequest())
    z.extractall(ziploc) # IDK more zip file stuff

    with open (ziploc+'/'+os.listdir(ziploc)[0],'r') as infile: # Read .csv file in unzipped directory
        a = csv.DictReader(infile)
        for item in a:
            outlist.append(item)
    shutil.rmtree(ziploc) # Delete non-empty directory after reading
    return(outlist)
  
@trackcalls  
def asynccheck(id,url):
    print('Checking if file is created')
    token = os.environ['zohotoken']
    headers = {'Authorization':f"Zoho-oauthtoken {token}"}
    r = requests.request('GET',url+'/'+id,headers=headers)
    if r.json()['details']['status']=='Completed':
        datalist = zohofiledownload(url,id)
        return(datalist)
    elif r.json()['details']['status']=='Failed':
        print('Download Failed')
        return(False)
    else:
        return(None)

@trackcalls
async def hitslackpaginated(method,url,payload,accumulator,output,cursor,triesremaining):
    if cursor != '':
        await asyncio.sleep(1)
    # cursoor needs to start out as ''
    # Accumulator is the key that the Slack API will respond back with that you want collected.
    # For example, hitting conversations.list returns a dictionary with keys ok, channels, and response_metadata. Channels holds the meat and potatoes of what I want, so accumulator should be 'channels'
    #headers.update({'Content-Type': 'application/json'})

    a = hitslacknojson(method,url,payload)
    aj = a.json()
    for returneddict in aj[accumulator]:
        output.append(returneddict)
    try:
        nextcursor = aj['response_metadata']['next_cursor']
        if nextcursor != cursor and nextcursor != '' and triesremaining>1:
            payload.update({'cursor':nextcursor})
            await hitslackpaginated(method,url,payload,accumulator,output,nextcursor,triesremaining-1)
        else:
            return(output)
    except KeyError:
        return(output)
    return(output) 

@trackcalls
async def idlookup(idcollection,conn):  
    print('idlookup')
    if idcollection == []:
        return([])
    datadict = {}
    for iddict in idcollection:
        devvehbase = 'SELECT devices.imei,devices.ismi,vehicles.fleetnumber,vehicles.vin,vehicles.itemid FROM vehicles INNER JOIN devices ON devices.ismi=vehicles.ismi where '
        devicebase = 'select ismi,imei from devices where ' 
        vehbase = 'select vin,fleetnumber,itemid from vehicles where '
        datapt = 0
        if iddict['ismi']!='':
            querysuffix = 'devices.ismi = %s'
            datapt = iddict['ismi']
            pttype = 'ismi'
        elif iddict['imei']!='':
            querysuffix = 'devices.imei = %s'
            datapt = iddict['imei']
            pttype = 'imei'
        elif iddict['vin']!='':
            querysuffix = 'vehicles.vin = %s'
            datapt = iddict['vin']
            pttype = 'vin'
        else:
            querysuffix = 'vehicles.fleetnumber = %s'
            datapt = iddict['fleetnumber']
            pttype = 'fleetnumber'
        try:
            query = devvehbase+querysuffix
            b = executequery(conn,query,[datapt])[0]
        except IndexError:
            if pttype in ['imei','ismi']:
                try:
                    query = devicebase+querysuffix
                    b = executequery(conn,query,[datapt])[0]
                    b.update({'fleetnumber':'','vin':'','itemid':''})
                except IndexError:
                    print('Unknown identifier',datapt,'of type',pttype)
            elif pttype in ['fleetnumber','vin']:
                try:
                    query = vehbase+querysuffix
                    b = executequery(conn,query,[datapt])[0]
                    b.update({'ismi':'','imei':''})
                except IndexError:
                    print('Unknown identifier',datapt,'of type',pttype) 
#        The hashing step needs to compute the dictionary without the source key and value, but then add them back in after
        hash = (hashlib.sha1(json.dumps(b).encode()).hexdigest())
        b.update({'source':iddict['source']})
        datadict.update({hash:b}) # Hash is the de-duplicating technique
#    print('idlookup values',list(datadict.values()))
    return(list(datadict.values()))

@trackcalls
async def findkeys(conn,inp,source):
#    print('finding keys')
    # This takes .03-.2 seconds to run for a given message
#    print(inp)
#    print(type(inp))
    # Implement PKP on strings 6 or longer with the regex noted
    # strings of length 6 have a collision rate of about 4%
    # strings with length 5 have collision rate of 30%, and a lot of them are multi collisions, which would lead to >=3 context bot posts
    # If you assume any partial key is either a VIN or an ISMI (probably wise), the collision rate goes down to 4.6% with few multi-collisions.
    # Lets hold off on slice 5 PKP
    # PKP Slice 6 with regex
    # PKP slice 5 limited to ismis and VIN. Don't do this
    keyfound = False
    prelimidcollection = []
    potentialnonfn=[]
    possibledelimiters = ['#',':',';','-','/','?','.',',','\n']
    if type(inp) == str:
        inp = inp.upper()
        for delim in possibledelimiters:
            inp = inp.replace(delim,' ')
        chunks =  inp.split(' ')
    elif type(inp) == list:
        chunks = inp
    else:
        print('findkeys requires a string or list as an input')
        return(False)

    regb = re.compile(r'^(?=.*\d)[A-Z\d]{6,}') # omits slack IDs, includes phone numbers. Oh well
    regf = re.compile(r'^[A-Z]{1,3}\d{1,4}$') # Catches a large variety of fleet numbers. BR3, PF1029, P2443, HOP148, DN351

    potential = [s for s in chunks if regb.search(s)]
    potentialfleetnumbers = [s for s in chunks if regf.search(s)]

    for chunk in chunks:
        if chunk == '53': # Toa ccount for the one vehicle in the fleet that we can never get rid of
            potentialfleetnumbers.append(chunk)

    for pfn in potentialfleetnumbers:
        a = executequery(conn,'SELECT vin,fleetnumber from vehicles where fleetnumber like %s',[pfn.upper()])
        if a!=[]:
            prelimidcollection.append({'ismi':'','imei':'','vin':a[0]['vin'],'fleetnumber':a[0]['fleetnumber']})

    for pgen in potential:
        if pgen not in potentialfleetnumbers:
            potentialnonfn.append(pgen)

    # This loop tries to match the potential key with a known key. It will take as much info as you throw at it. A 6 length key may result in a collision, but posting the entire key is certain to yield a correct match
    for pot in potentialnonfn:
        keylen = len(pot)
        a = executequery(conn,'SELECT vin,fleetnumber from vehicles where RIGHT(vin,%s)=%s',[keylen,pot])
        if a!=[]:
            prelimidcollection.append({'ismi':'','imei':'','vin':a[0]['vin'],'fleetnumber':a[0]['fleetnumber']})
        else:
            b = executequery(conn,'SELECT ismi,imei from devices where RIGHT(ismi,%s)=%s',[keylen,pot])
            if b!=[]:
                prelimidcollection.append({'ismi':b[0]['ismi'],'imei':b[0]['imei'],'vin':'','fleetnumber':''})
            else:
                c = executequery(conn,'SELECT ismi,imei from devices where RIGHT(imei,%s)=%s',[keylen,pot])
                if c!=[]:
                    prelimidcollection.append({'ismi':c[0]['ismi'],'imei':c[0]['imei'],'vin':'','fleetnumber':''})


    for item in prelimidcollection:
        item.update({'source':source})
#        print(item)
#    print('end of findkeys')
    if len(prelimidcollection)>0:
        keyfound = True
    return(keyfound,prelimidcollection)

@trackcalls
async def fotaasyncrequest(url,payload,waittime):
#    urlr = 'https://api.teltonika.lt/devices/export'
    headers = {"Authorization": os.environ['fotatoken'],'Accept':'application/json','Content-Type':'application/json'}
     # Tell FOTA what devices I want exporte
    print('Requesting list>csv from FOTA')
    r = requests.request('POST',url,headers=headers,data=json.dumps(payload))
    if r.status_code!=200:
        return(0)
    await asyncio.sleep(waittime)
    urls = 'https://api.teltonika.lt/files?sort=created_at&order=desc'  # This call doesn't like the payload for some reason
    filepayload = {
    #    'company_id':[6865],
        'sort':'created_at',
        'order':'desc',
    }
    # Search files by created_at desc to find fileid
    print('Searching for csv')
    s = requests.request('GET',urls,headers=headers) 
    if s.status_code!=200:
        return(0)
    fileid = str(s.json()['data'][0]['id'])
    # Download fileid from FOTA
    print(f"Downloading csv with fileid {fileid}")
    urlt = 'https://api.teltonika.lt/files/download/'+fileid
    t = requests.request('GET',urlt,headers=headers)
    if t.status_code!=200:
        return(0)
    # Save file
    saveloc = os.path.join(os.environ['workingdir'],f"fotafile-{fileid}.csv")
#    saveloc = f"{home}/workscripts/connections/fotadownloadfiles/fotafile-{fileid}.csv"
    open(saveloc,'w').write(t.text)
    datalist = []
    with open(saveloc,'r') as a:
        b = csv.DictReader(a)
        for row in b:
            datalist.append(row)
    os.remove(saveloc)
    return(datalist)

@trackcalls
def hitfota(method,url,headers,payload,jsonbool):
    if headers == {}:
        headers.update({"Authorization": os.environ['fotatoken'],'Accept':'application/json','Content-Type':'application/json'})
    else:
        headers.update({"Authorization": os.environ['fotatoken']})
    if jsonbool == False:
        r = requests.request(method,url,headers=headers,params = payload)
    else:
        r = requests.request(method,url,headers=headers,data=json.dumps(payload))
    return(r)

@trackcalls
def timechanger(intime,intzstr,outtzstr):
    intz = pytz.timezone(intzstr)
    outtz = pytz.timezone(outtzstr)
    intzlocalize = intz.localize(intime)
    outtime = intzlocalize.astimezone(outtz)
    return(outtime)

@trackcalls
def getcommandaudit(ismi,start,end):
    # times are utc
    start = datetime.datetime.strftime(start,'%m/%d/%y %H:%M:%S').replace('/','%2F').replace(' ','%20').replace(':','%3A')
    end = datetime.datetime.strftime(end,'%m/%d/%y %H:%M:%S').replace('/','%2F').replace(' ','%20').replace(':','%3A')
    url = f"{os.environ['ccurl']}/responses/commandAudits/{ismi}?startDateUTC={start}&endDateUTC={end}"
    r = requests.request('GET',url,headers = {'Authorization':f"Basic {os.environ['cctoken']}"})
    return(r)

@trackcalls
def initializeopenai():
    # For initializing the OpenAI method
    from openai import OpenAI as oai
    client = oai(organization=os.environ['openaiorgid'])
    openai = client.beta
    return(openai)

@trackcalls
def interactai(openai,arglist,args):
    '''
    This funciton takes the initialized open AI object, adds on the methods (e.g. threads.messages.list), adds in the keyword arguments, calls the built funciton, and returns the response
    Example use:
    arglist = ['threads','messages','list']
    kwargs = {'thread_id':threadid}
    openai = client.beta
    interactai(openai,arglist,kwargs)
    '''
    for arg in arglist:
        openai = getattr(openai,arg)
    response = openai(**args)
    return(response)

@trackcalls
def uploadaifile(filename):
    # Remember, you can upload a .csv to OpenAI Storage, but you can't use a .csv for retrieval with an Assistant
    # https://platform.openai.com/docs/assistants/tools/supported-files
    from openai import OpenAI as oai
    client = oai(organization=os.environ['openaiorgid'])
    if filename[-4:] == '.csv':
        infilename = filename
        outfilename = filename.replace('.csv','.json')
    else:
        infilename = filename
        outfilename = outfilename

    fileresponse = client.files.list()
    dupeexists = False
    dupeid = ''
    for file in fileresponse.data:
        if outfilename == file.filename:
            dupeexists = True
            dupeid = file.id
    if dupeexists == False:
        if infilename !=outfilename:
            print('Converting file to csv')
            csvdata = []
            with open(infilename,'r') as incsv:
                b = csv.DictReader(incsv)
                for c in b:
                    csvdata.append(c)
            with open(outfilename,'w') as outfile:
                json.dump(csvdata,outfile,indent=2)

        with open(outfilename,'rb') as file:
            response = client.files.create(file=file, purpose="assistants")
        print(f"Uploaded {outfilename} to OpenAI Storage")
        return(response.id)
    else:
        print(f"{outfilename} already exists in Open AI Storage")
        return(dupeid)

@trackcalls
def datetranslator(datestr,patterns):
    for pattern in patterns:
        try:
            return(datetime.datetime.strptime(datestr,pattern))
        except ValueError:
            pass
    return(None)

def hitadminprod(method,urlsuffix,payload,jsonbool):
    url = os.environ['adminprodbaseurl']+urlsuffix
    headers = {'Content-Type':'application/json','Authorization':'Bearer '+os.environ['adminprodtoken']}
    if payload == {}:
        r = requests.request(method,url, headers = headers)
    else:
        if jsonbool == True:
            r = requests.request(method,url, headers = headers,data=json.dumps(payload))
        else:
            headers.update({'Content-Type':'xxx-form-urlencoded'})
            r = requests.request(method,url, headers = headers,params=payload)

    return(r)

def readgsheet(sheetidentifier):
    filepath = os.environ['googledrivecreds']
    gc = gspread.service_account(filename=filepath)
    if 'https://' in sheetidentifier:
        sh = gc.open_by_url(sheetidentifier)
    else:
        sh = gc.open(sheetidentifier)
    return(sh)

def getids(conn,keys):
    if not conn:
        conn = dbconn('work')
    # Conn is optional
    # Keys can be string or list
    # Requires a load_dotenv
    # A function to take a list or string, extract the potential ids, do the findkey thing on them, do the idlookup on them, and return as much info as I have
    fko = asyncio.run(findkeys(conn,keys,''))
    ico = asyncio.run(idlookup(fko[1],conn))
    return(ico)

def hitcommandcenter(method,urlsuffix,headers,payload,jsonbool):
    url = os.environ['ccurl']+'/'+urlsuffix 
    headers.update({'Authorization':f"Basic {os.environ['cctoken']}"})
    print(url)
    r = requests.request(method,url,headers=headers)
    return(r)