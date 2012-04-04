import threading
import time
from threading import Thread
from datetime import datetime
from os import path
from pytz import timezone, UTC, utc
from time import strftime, strptime, gmtime, mktime, tzset
import os
import sys

from trac.util.datefmt import localtz
from trac.core import *
from trac.search import ISearchSource
from trac.config import Option, IntOption

import web_ui
from api import IIRCLogIndexer, IRCChannelManager

whoosh_loaded = False
try:
    from whoosh.filedb.filestore import FileStorage
    from whoosh.fields import Schema, TEXT, STORED
    from whoosh.qparser import QueryParser
    from whoosh import index
    whoosh_loaded = True
except Exception, e:
    sys.__stderr__.write(
            "WARNING: Failed to load whoosh library.  Whoosh index disabled")
    sys.__stderr__.write(e.message)

if whoosh_loaded:


    class IrcUpdater(Component):
        
        """Run IRC update sequence"""
        indexpath = Option('irclogs', 'search_db_path', 'irclogs-index',
                doc="Location of irclogs whoosh index")
        last_index = IntOption('irclogs', 'last_index', 239414400,
                doc="Epoch of last index.  Aug 7th, 1977 GMT by default.")
        TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'
        SCHEMA = Schema(channel=STORED, timestamp=STORED, 
                content=TEXT(stored=True))
        PARSER = QueryParser("content", schema=SCHEMA)
        
        def start(self):
            self.myThread = TimeThread(self.update_index, 30, self.log)
            self.myThread.setName("indexThread")
            self.myThread.start()

        def get_index(self):
            ip = self.indexpath
            if not self.indexpath.startswith('/'):
                ip = path.join(self.env.path, ip)
            if not path.exists(ip):
                os.mkdir(ip)
            if not index.exists_in(ip):
                index.create_in(ip, self.SCHEMA)
            return index.open_dir(ip)

        def update_index(self):

            print "updating index.."
            last_index_dt = UTC.localize(datetime(*gmtime(self.last_index)[:6]))
            now = UTC.localize(datetime.utcnow())
            idx = self.get_index()
            writer = idx.writer()
            try:
                chmgr = IRCChannelManager(self.env)
                self.log.debug("chmanager")
                self.log.debug(chmgr)
                for channel in chmgr.channels():
                    self.log.debug("channel: ")
                    self.log.debug(channel.name())
                    for line in channel.events_in_range(last_index_dt, now):
                        if line['type'] == 'comment': 
                            content = "<%s> %s"%(line['nick'], 
                                    line['comment'])
                            writer.add_document(
                                    channel=channel.name(),
                                    timestamp=line['timestamp'].strftime(
                                        self.TIMESTAMP_FORMAT),
                                    content=content
                                    )
                            #print content
                        if line['type'] == 'action':
                            content = "* %s %s"%(line['nick'], line['action'])
                            writer.add_document(
                                    channel=channel.name(),
                                    timestamp=line['timestamp'].strftime(
                                        self.TIMESTAMP_FORMAT),
                                    content=content
                                    )
                            # START BULLSHIT
                # Python can't turn a nonlocal datetime to a epoch time AFIACT
                # This pisses me off to no end.  Who knows what kind of fucked
                # up side effects this has.
                os.environ['TZ'] = 'UTC'
                tzset()
                # END BULLSHIT
                epoch_now = int(mktime(now.timetuple()))
                self.config['irclogs'].set('last_index', epoch_now)
                self.config.save()
                writer.commit()
                idx.close()
            except Exception, e:
                self.log.debug(e)
                writer.commit()
                #writer.cancel()
                idx.close()
    #===============================================================================
# Timer Implementation
#===============================================================================
#===========================================================================
# Timer Implementation with execute a given function in an given intervall
# @param func: Function to call
# @param sec: Sleep intervall in seconds
#===========================================================================
    class TimeThread(Thread):
        def __init__(self, func, sec=2, log_func=None):
            Thread.__init__(self)
            self.func = func
            self.sec = sec
            self.log = log_func
            if self.log:
                self.log.info('XMailTimerThread: init done with sec: %s' % sec)

        def run(self):
                try:
                    # error occured: 'ascii' codec can't encode character u'\xfc' in position 19: ordinal not in range(128)
                    self.func()
                except Exception, e:
                    if self.log:
                        self.log.error('==============================\n' \
                                '[XMailTimerThread.run] -- Exception occured: %r' % e)
                        exc_traceback = sys.exc_info()[2]
                        self.log.error('TraceBack: %s' % exc_traceback )

    class WhooshIrcLogsIndex(Component):
        implements(ISearchSource)
        implements(IIRCLogIndexer)

        TIMESTAMP_FORMAT = '%Y%m%d%H%M%S'
        SCHEMA = Schema(channel=STORED, timestamp=STORED, 
                content=TEXT(stored=True))
        PARSER = QueryParser("content", schema=SCHEMA)

        indexpath = Option('irclogs', 'search_db_path', 'irclogs-index',
                doc="Location of irclogs whoosh index")
        last_index = IntOption('irclogs', 'last_index', 239414400,
                doc="Epoch of last index.  Aug 7th, 1977 GMT by default.")

        def __init__(self):
            self.updater = IrcUpdater(self.compmgr)

        # Start ISearchSource impl
        def get_search_filters(self, req):
            ch_mgr = IRCChannelManager(self.env)
            for channel in ch_mgr.channels():
                if req.perm.has_permission(channel.perm()):
                    return [('irclogs', 'IRC Logs', True)]
            return []

        def get_search_results(self, req, terms, filters):
            # cache perm checks to speed things up
            permcache = {}
            chmgr = IRCChannelManager(self.env)

            if not 'irclogs' in filters:
                return

            #logview = web_ui.IrcLogsView(self.env)
            for result in self.search(terms):
                dt_str = ''
                d_str = ''
                t_str = ''
                if result.get('timestamp'):
                    print "timestamp: "+result['timestamp']
                    d_str = "%04s/%02s/%02s" % (result['timestamp'][0:4], result['timestamp'][4:6],result['timestamp'][6:8])
                    print d_str
                    t_str = "%02s:%02s:%02s" % (result['timestamp'][8:10], result['timestamp'][10:12],result['timestamp'][12:14])
                    dt = datetime(int(result['timestamp'][0:4]), int(result['timestamp'][4:6]),int(result['timestamp'][6:8]),int(result['timestamp'][8:10]), int(result['timestamp'][10:12]),int(result['timestamp'][12:14]),tzinfo=utc)

                    #t_str = time_tuple[3]+":"+time_tuple[4]+":"+time_tuple[5]

                    #dt = chmgr.to_user_tz(req, result['timestamp'])
                    #d_str = "%04d/%02d/%02d"%(
                    #    dt.year,
                    #    dt.month,
                    #    dt.day,
                    #)
                    #t_str = "%02d:%02d:%02d"%(
                    #    dt.hour,
                    #    dt.minute,
                    #    dt.second
                    #)
                channel = ''
                self.log.debug("result")
                self.log.debug(result)
                if result.get('channel'):
                    channel = '%s/'%result['channel']

                url = '/irclogs/%s%s'%(channel, d_str)
                if not permcache.has_key(channel):
                  #  chobj = chmgr.channel(result['channel'])
                    chobj = chmgr.channel(channel)
                    permcache[channel] = req.perm.has_permission(chobj.perm())
                if permcache[channel]:
                    yield "%s#%s"%(req.href(url), t_str), \
                            'irclogs for %s'%channel, dt ,'irclog', result['content']
        # End ISearchSource impl



        def get_index(self):
            ip = self.indexpath
            if not self.indexpath.startswith('/'):
                ip = path.join(self.env.path, ip)
            if not path.exists(ip):
                os.mkdir(ip)
            if not index.exists_in(ip):
                index.create_in(ip, self.SCHEMA)
            return index.open_dir(ip)

        def search(self, terms):
            self.updater.start()
            chmgr = IRCChannelManager(self.env)
            ix = self.get_index()
            self.log.debug(ix)
            searcher = ix.searcher()
            self.log.debug(searcher)
            parsed_terms = self.PARSER.parse(' or '.join(terms))
            self.log.debug("terms "+parsed_terms.__str__())
            if terms:
                for f in searcher.search(parsed_terms):
                    timestamp = strptime(f['timestamp'], self.TIMESTAMP_FORMAT)
                    #f['timestamp'] = \
                            #        UTC.localize(datetime(*timestamp[:6]))
                    yield f
