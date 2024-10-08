import asyncio
import psutil
import datetime
import inspect
import traceback
from datetime import timedelta
try:
    from database import Database
except ModuleNotFoundError:
    try:
        from support_connections.database import Database
    except ModuleNotFoundError:
        pass

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


async def runpull(pullfunc,alteredentity,entitytype,dependencies):
    db = Database()
    # dependencies is a dictionary of processname:integer-seconds-since-update
    start = datetime.datetime.now()
    stats = []
    conn = db.connect('functiondb')
    try:
        # This second checks for successfully-run dependencies and raises an error if any dependencies don't match
        for processname,secondssinceupdate in dependencies.items():
            datelimit = start-timedelta(seconds=secondssinceupdate)
            depend_query = """
                SELECT * FROM functionstate
                WHERE successbool = 1
                AND processname = %s
                AND lastranat >= %s
                """
            # Make sure to use autoconnect here in case there are long running queries that would otherwise make the conneciton timeout

            depend_results = Database().execute_query(depend_query,(processname,datelimit),database = 'functiondb', autoconnect = True)
            print('depend_results',depend_results)
#            processq = executequeryoc('functiondb','SELECT * FROM functionstate WHERE successbool = 1 AND processname = %s AND lastranat >= %s',[processname,datelimit])
            if depend_results == []:
                output = f"Dependent process {processname} has not successfully run within the last {secondssinceupdate/60} minutes"
                raise ValueError(output)
            
        # This actually await runs the function
        if inspect.iscoroutinefunction(pullfunc):
            stattask = asyncio.create_task(getstats(1,stats))
            output = await pullfunc()
            stattask.cancel()
            if stats != []:
                stats_query = """
                    INSERT INTO computingstate
                    (datetime,cpuuse,memoryuse)
                    VALUES (?, ?, ?)
                    ON DUPLICATE KEY UPDATE datetime=datetime
                """
                Database().write_many(stats_query,stats,database = 'functiondb', autoconnect = True)
        else:
            raise TypeError(f"{pullfunc.__name__} must be an async coroutine")
        successbool = True
        
    except Exception as e:
        traceback.print_exc()
        output = str(e) # Make sure to stringify it, or you won't be able to insert it in the table
        print('Exception',e)
        successbool = False
    end = datetime.datetime.now()
    duration = (end-start).total_seconds()
    state_query = """
        INSERT INTO functionstate 
        (processname, alteredentity, entitytype, lastranat,
        runduration,successbool,output) 
        VALUES (?,?,?,?,?,?,?) 
        on duplicate key update processname=VALUES(processname),
        alteredentity=VALUES(alteredentity),entitytype=VALUES(entitytype),
        lastranat=VALUES(lastranat),runduration=VALUES(runduration),
        successbool=VALUES(successbool),output=VALUES(output)
        """
    state_data = [
        pullfunc.__name__,
        alteredentity,
        entitytype,
        start,
        duration,
        successbool,
        output,
    ]
    summary_query = """
    INSERT INTO functionsummary 
    (functionname, starttime, endtime, duration, output, successbool, type)
    VALUES (?,?,?,?,?,?,?)
    """
    summary_data = [
        pullfunc.__name__,
        start,
        end,
        duration,
        output,
        successbool,
        entitytype
    ]
    db.execute_query(state_query,state_data)
    db.execute_query(summary_query,summary_data)



if __name__ == '__main__':
    async def test():
        return('test')
    asyncio.run(runpull(test,'test','test',{}))