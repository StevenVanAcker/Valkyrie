#!/usr/bin/env python3

import time
import docker
import boto3, botocore
import pprint
import threading
import json
import copy
import subprocess
import collections
import logging

class ValkyrieCommon():
        SQS_QUEUE_MASTER = "valkyrieMaster"

        def __init__(self): #{{{
            self.logger = logging.getLogger(self.__class__.__name__)
            self.logger.setLevel(logging.WARNING)
            ch = logging.StreamHandler()
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)
#}}}
        def get_maybe_create_queue(self, queuename): #{{{
            sqs = boto3.resource('sqs') 
            inq = None
            try:
                inq = sqs.get_queue_by_name(QueueName=queuename)
                return inq
            except Exception as e:
                inq = sqs.create_queue(QueueName=queuename)
            return inq
#}}}

# threads:
# masterThread (main)
#    receives messages from master and handles them
#       event-driven!
#       sets new recipe, signals maintenanceThread and dockerFetchThread
# statusThread
#    sends status to master every 5 seconds or on SIGNAL from any thread: master/dockerfetch/maintenance
#       timer driven!
#       read-only: only reads data from state, and send it to remote
# maintenanceThread
#    make sure the running instances are in sync with recipe: every N seconds or after SIGNAL from master/dockerfetch
#       timer driven!
#       starts/stops some instances if needed
# dockerFetchThread
#    every N seconds or after SIGNAL from master thread
#    ensures all docker images in the recipe are available and signals maintenanceThread/statusThread when download has finished
#       
# dockerRunThread -- this is hidden inside _startSingleDockerInstance
#    run a single docker container until done
#    signal maintenance thread when done

class ValkyrieSlave(ValkyrieCommon):
        MAINTENANCETHREAD_SLEEP = 3 # 12099
        DOCKERFETCHTHREAD_SLEEP = 3099
        MASTERTHREAD_SLEEP = 0
        STATUSTHREAD_SLEEP = 1399

        def __init__(self, queue): #{{{
            super().__init__()

            self.queue = queue
            self.recipe = {}
            self.dockerFetch_signal_condition = threading.Condition()
            self.maintenance_signal_condition = threading.Condition()

            self.kill_everything = False
#}}}
        def getStatus(self): # FIXME {{{
                # count running instances
                # count free ram, diskspace, cpu, ...
                # report last error per image type?
                pass
#}}}
        def communicate(self, status): # FIXME {{{
                # send status to master on SQS_QUEUE_NAME_MASTER
                # read all messages from own queue, only process latest one (check timestamp)
                # store master's request: how many instances from each type? + where to get them + which parameters
                # should there be a hard kill or not?

                # should this run in a separate thread?
                pass
#}}}
        def fetchDockerImages(self, localRecipe): #{{{
            """
            Go over all images in the recipe and download those that are missing.
            """
            try:
                loggedIn = False

                for (imagename, values) in localRecipe.items():
                        if not self._dockerImageExists(imagename):
                                if not loggedIn:
                                        loggedIn = self._loginDocker()

                                if loggedIn:
                                    if self._fetchDockerImage(values["repo"], imagename):
                                        self.logger.info("fetchDockerImages(): successfully fetched image {}".format(imagename))
                                        self._signalMaintenanceThread()
                                    else:
                                        self.logger.error("fetchDockerImages(): failed to fetch image {}".format(imagename))
                                else:
                                    self.logger.error("fetchDockerImages(): docker not logged in :(")

                self.logger.debug("fetchDockerImages(): done updating docker images")
            except Exception as e:
                self.logger.error("fetchDockerImages() threw an exception: {}".format(e))
#}}}
        def processMessageFromMaster(self): # FIXME {{{
            """
            Listen to this host's SQS queue for updates from the master
            and handle the messages.
            Messages are:
                    updateRecipe to set the new recipe

            Returns True if anything was changed (like changing the recipe)
            """
            try:
                sqs = boto3.resource('sqs') 
                inq = self.get_maybe_create_queue(self.queue)

                for msg in inq.receive_messages(WaitTimeSeconds = self.MASTERTHREAD_SLEEP, MaxNumberOfMessages=1, MessageAttributeNames=['All']):
                    self.logger.debug("Message received: {}".format(msg.body))
                    msgdata = json.loads(msg.body)
                    msg.delete()

                    # return True if the recipe was updated
                    if msgdata["type"] == "updateRecipe":
                        with self.maintenance_signal_condition:
                            with self.dockerFetch_signal_condition:
                                self.recipe = msgdata["recipe"]
                                self._signalMaintenanceThread()
                                self._signalDockerFetchThread()
                        return True
                    if msgdata["type"] == "shutdown":
                        self.logger.error("processMessageFromMaster() received shutdown signal")
                        self.kill_everything = True
                        time.sleep(100)
                        return True

                    return False
            except botocore.exceptions.NoRegionError as e:
                self.logger.error("processMessageFromMaster() received NoRegionError from boto3: {}".format(e))
                self.kill_everything = True
                time.sleep(1)
            except Exception as e:
                self.logger.error("processMessageFromMaster() threw an exception: {}".format(e))
                return False
#}}}
        def realizeRecipe(self, localRecipe): # {{{
            """
            Apply the recipe, starting and stopping instances as needed.
            Beware: Some images may not be downloaded yet!
            """
            threads = []
            allInstances = self._dockerInstances()
            for (imagename, values) in localRecipe.items():
                    while len(allInstances[imagename]) != values["amount"]:
                        if len(allInstances[imagename]) > values["amount"]:
                            if "kill" in values and values["kill"]:
                                dockerInstance = allInstances[imagename][0]
                                # stopping an instance takes a while and can be
                                # run in threads in parallel, but afterwards,
                                # we need to join() all these threads before
                                # returning from realizeRecipe()
                                threads += [self._stopSingleDockerInstance(dockerInstance)]
                                self.logger.error("realizeRecipe(): !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! stopping an instance of {}".format(imagename))
                            else:
                                self.logger.error("realizeRecipe(): !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! should stop an instance of {}, but 'kill' option not specified...".format(imagename))
                            allInstances[imagename] = allInstances[imagename][1:]
                        if len(allInstances[imagename]) < values["amount"]:
                            c = self._startSingleDockerInstance(imagename)
                            allInstances[imagename] += [c]
                            self.logger.debug("realizeRecipe(): started an instance of {}".format(imagename))

            if len(threads) > 0:
                    self.logger.debug("realizeRecipe(): waiting until instances are stopped...")
                    [x.join() for x in threads]
                    self.logger.debug("realizeRecipe(): Done.")
#}}}

        def _loginDocker(self): #{{{
                while True:
                    try:
                        ret = subprocess.check_call("$(aws ecr get-login)", stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                        return True
                    except subprocess.CalledProcessError as e:
                        self.logger.error("_loginDocker() threw error: {}".format(e))
                        time.sleep(0.1)
#}}}
        def _dockerImageExists(self, imagename): # {{{
                try:
                    # init docker client
                    d = docker.from_env()
                except Exception as e:
                    self.logger.error("_dockerImageExists() threw exception: {}".format(e))

                try:
                    # try to get info about the image
                    im = d.images.get(imagename)
                    return True
                except:
                    # if there is an exception, the image doesn't exist
                    pass

                return False
#}}}
        def _fetchDockerImage(self, repo, imagename): # {{{
                try:
                    # init docker client
                    d = docker.from_env()
                    # pull from repo
                    im = d.images.pull(repo)
                    # tag image
                    im.tag(imagename)
                    return True
                except Exception as e:
                    self.logger.error("_fetchDockerImage() threw exception: {}".format(e))
                return False
#}}}
        def _dockerInstances(self): # {{{
                while True:
                    try:
                        out = collections.defaultdict(list)
                        # init docker client
                        d = docker.from_env()
                        # add running containers
                        for c in d.containers.list():
                            if c.status in ["created", "restarting", "running"]: # not "removing", "paused", "exited", "dead"
                                out[c.attrs["Config"]["Image"]] += [c]
                        return out
                    except Exception as e:
                        self.logger.error("_dockerInstanceCounts() threw exception: {}".format(e))
                    time. sleep(0.1)
#}}}
        def _startSingleDockerInstance(self, imagename): # {{{
                def _startSingleDockerInstance_inner(self, imagename):
                    try:
                        self.logger.debug("Start running container {}".format(imagename))
                        # init docker client
                        d = docker.from_env()
                        # run containers
                        try:
                            d.containers.run(imagename, remove=True)
                        except Exception as e:
                            self.logger.debug("_startSingleDockerInstance() during run threw exception: {}".format(e))
                        self._signalMaintenanceThread()
                        self.logger.debug("Done running container {}".format(imagename))
                    except Exception as e:
                        self.logger.error("_startSingleDockerInstance() threw exception: {}".format(e))

                runThread = threading.Thread(target=_startSingleDockerInstance_inner, args=(self, imagename,))
                runThread.daemon = True
                runThread.start()

                # FIXME: because auto restart does not work with auto remove,
                # may need to start a thread to run docker without detach, and
                # then send a signal to the maintenance thread when the
                # container stops running
                return None
#}}}
        def _stopSingleDockerInstance(self, dockerInstance): # {{{
            def _stopSingleDockerInstance_inner(self, dockerInstance):
                try:
                    dockerInstance.stop(timeout=5)
                except Exception as e:
                    pass

            # this can take a while, so run it in a thread and join() on this thread later
            t = threading.Thread(target=_stopSingleDockerInstance_inner, args=(self, dockerInstance))
            t.daemon = True
            t.start()
            return t
#}}}
        def _signalMaintenanceThread(self): #{{{
            with self.maintenance_signal_condition:
                self.maintenance_signal_condition.notifyAll()
#}}}
        def _signalDockerFetchThread(self): #{{{
            with self.dockerFetch_signal_condition:
                self.dockerFetch_signal_condition.notifyAll()
#}}}

        def _masterThreadLoop(self): # {{{
            # listen for messages from master and react to them
            while True:
                self.processMessageFromMaster()
#}}}
        def _statusThreadLoop(self): #FIXME{{{
            while True:
                self.logger.debug("_statusThreadLoop() says hi")
                time.sleep(self.STATUSTHREAD_SLEEP)
#}}}
        def _dockerFetchThreadLoop(self): #{{{
            while True:
                localRecipe = None
                with self.dockerFetch_signal_condition:
                    self.logger.debug("_dockerFetchThreadLoop(): sleeping for {}...".format(self.DOCKERFETCHTHREAD_SLEEP))
                    self.dockerFetch_signal_condition.wait(self.DOCKERFETCHTHREAD_SLEEP) # wait for an update or timeout
                    self.logger.debug("_dockerFetchThreadLoop(): just woke up!")
                    localRecipe = copy.deepcopy(self.recipe)
                self.fetchDockerImages(localRecipe)
#}}}
        def _maintenanceThreadLoop(self): #{{{
            while True:
                localRecipe = None
                with self.maintenance_signal_condition:
                    self.logger.debug("_maintenanceThreadLoop(): sleeping for {}...".format(self.MAINTENANCETHREAD_SLEEP))
                    self.maintenance_signal_condition.wait(self.MAINTENANCETHREAD_SLEEP) # wait for an update or timeout
                    self.logger.debug("_maintenanceThreadLoop(): just woke up!")
                    localRecipe = copy.deepcopy(self.recipe)
                self.realizeRecipe(localRecipe)
                time.sleep(2) # settle dust
#}}}
        def run(self): #{{{
                self.statusThread = threading.Thread(target=self._statusThreadLoop)
                self.statusThread.daemon = True
                self.statusThread.start()
                self.logger.info("Started statusThread")

                self.dockerFetchThread = threading.Thread(target=self._dockerFetchThreadLoop)
                self.dockerFetchThread.daemon = True
                self.dockerFetchThread.start()
                self.logger.info("Started dockerFetchThread")

                self.maintenanceThread = threading.Thread(target=self._maintenanceThreadLoop)
                self.maintenanceThread.daemon = True
                self.maintenanceThread.start()
                self.logger.info("Started maintenanceThread")

                self.masterThread = threading.Thread(target=self._masterThreadLoop)
                self.masterThread.daemon = True
                self.masterThread.start()
                self.logger.info("Started masterThread")

                while not self.kill_everything:
                    time.sleep(1)

                self.logger.debug("Exit from main loop because self.kill_everything == True")
#}}}


# loop and read all messages, then send same recipe to all hosts
class ValkyrieMaster(ValkyrieCommon):
        MASTERTHREAD_SLEEP = 2

        def __init__(self, recipe): #{{{
            super().__init__()
            self.recipe = recipe
            self.kill_everything = False
#}}}
        def processMessageFromSlaves(self): # FIXME {{{
            try:
                sqs = boto3.resource('sqs') 
                inq = self.get_maybe_create_queue(self.SQS_QUEUE_MASTER)

                for msg in inq.receive_messages(WaitTimeSeconds = self.MASTERTHREAD_SLEEP, MaxNumberOfMessages=1, MessageAttributeNames=['All']):
                    self.logger.debug("Message received: {}".format(msg.body))
                    msgdata = json.loads(msg.body)
                    msg.delete()

                    # return True if the recipe was updated
                    if msgdata["type"] == "updateRecipe":
                        return True

                    return False
            except botocore.exceptions.NoRegionError as e:
                self.logger.error("processMessageFromSlaves() received NoRegionError from boto3: {}".format(e))
                self.kill_everything = True
            except Exception as e:
                self.logger.error("processMessageFromSlaves() threw an exception: {}".format(e))
                return False
#}}}
        def run(self): #{{{
                while not self.kill_everything:
                    self.processMessageFromSlaves()
                    time.sleep(1)
#}}}

if __name__ == "__main__":
    import random, string
    rndqueue = "valkyrie-" + ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(16))

    slave = ValkyrieSlave(rndqueue)
    #slave.logger.setLevel(logging.INFO)
    slave.logger.setLevel(logging.DEBUG)
    slave.run()





