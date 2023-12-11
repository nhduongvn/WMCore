#!/usr/bin/env python
"""
_StageOutMgr_

Util class to provide stage out functionality as an interface object.

Based of RuntimeStageOut.StageOutManager, that should probably eventually
use this class as a basic API
"""
from __future__ import print_function
from builtins import object
from future.utils import viewitems

import logging
# If we don't import them, they cannot be ever used (bad PyCharm!)
import WMCore.Storage.Backends
import WMCore.Storage.Plugins

from WMCore.Storage.DeleteMgr import DeleteMgr
from WMCore.Storage.Registry import retrieveStageOutImpl
from WMCore.Storage.StageOutError import StageOutFailure
from WMCore.Storage.StageOutError import StageOutInitError
from WMCore.WMException import WMException

from WMCore.Storage.SiteLocalConfig import stageOutStr
from WMCore.Storage.RucioFileCatalog import storageJsonPath, readRFC 


def stageoutPolicyReport(fileToStage, rse, command, stageOutType, stageOutExit):
    """
    Prepare some extra information regarding the stage out step (for both prod/analysis jobs).

    NOTE: this information used to be shipped to the old SSB dashboard. I'm unsure
    whether it's provided to any other monitoring system at the moment.
    """
    tempDict = {}
    tempDict['LFN'] = fileToStage['LFN'] if 'LFN' in fileToStage else None
    tempDict['RSE'] = fileToStage['RSE'] if 'RSE' in fileToStage else None
    tempDict['RSE'] = rse if rse else tempDict['RSE']
    tempDict['StageOutCommand'] = fileToStage['command'] if 'command' in fileToStage else None
    tempDict['StageOutCommand'] = command if command else tempDict['StageOutCommand']
    tempDict['StageOutType'] = stageOutType
    tempDict['StageOutExit'] = stageOutExit
    fileToStage['StageOutReport'].append(tempDict)
    return fileToStage


def getPFN(rfc, lfn):
    """
    _getPFN_

    Get PFN for provided lfn using Rucio file catalog rfc
    Parameters:
    rfc: a Rucio file catalog
    lfn: logical file name
    Return:
    pfn: logical file name, if no match found return None
    """
    if rfc == None:
        msg = "Rucio File Catalog not available to match LFN:\n"
        msg += lfn
        logging.error(msg)
        return None
    if rfc.preferredProtocol == None:
        msg = "Rucio File Catalog: "+str(rfc)+"does not have a preferred protocol\n"
        msg += "which prevents stage out for:\n"
        msg += lfn
        logging.error(msg)
        return None

    pfn = rfc.matchLFN(rfc.preferredProtocol, lfn)
    if pfn == None:
        msg = "Unable to map LFN to PFN:\n"
        msg += "LFN: %s\n" % lfn
        msg += "using this Rucio File Catalog\n"
        msg += str(rfc)
        logging.error(msg)
        return None 

    msg = "LFN to PFN match made:\n"
    msg += "LFN: %s\nPFN: %s\n" % (lfn, pfn)
    logging.info(msg)
    
    return pfn


class StageOutMgr(object):
    """
    _StageOutMgr_

    Object that can be used to stage out a set of files
    using RFC or an override.

    """

    def __init__(self, **overrideParams):
        logging.info("StageOutMgr::__init__()")
        self.overrideConf = overrideParams

        # Figure out if any of the override parameters apply to stage-out
        self.override = False
        if overrideParams != {}:
            logging.info("StageOutMgr::__init__(): Override: %s", overrideParams)
            checkParams = ["command", "option", "rse", "lfn-prefix"]
            for param in checkParams:
                if param in self.overrideConf:
                    self.override = True
            if not self.override:
                logging.info("=======StageOut Override: These are not the parameters you are looking for")

        self.substituteGUID = True
        
        self.stageOuts_rfcs = [] #pairs of stageOut and Rucio file catalog

        self.numberOfRetries = 3
        self.retryPauseTime = 600

        from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

        #  //
        # // If override isnt None, we dont need SiteCfg, if it is
        # //  then we need siteCfg otherwise we are dead.

        if self.override == False:
            self.siteCfg = loadSiteLocalConfig()

        if self.override:
            self.initialiseOverride()
        else:
            self.initialiseSiteConf()
        
        #this is used for unittest only. Change value directly in the unittest
        self.bypassImpl = False
        self.failed = {}
        self.completedFiles = {}
        return

    def initialiseSiteConf(self):
        """
        _initialiseSiteConf_

        Extract required information from site conf and RFC

        """
        self.stageOuts = self.siteCfg.stageOuts

        msg = "\nThere are %s stage out definitions." % len(self.stageOuts)
        for stageOut in self.stageOuts:
            for k in ['rse','command','storageSite','volume','protocol']:
                v = stageOut.get(k)
                if v is None:
                    amsg = ""
                    amsg+= "Unable to retrieve "+k+" of this stageOut: \n"
                    amsg+= stageOutStr(stageOut) + "\n"
                    amsg+= "From site config file.\n"
                    amsg+= "Continue to the next stageOut.\n"
                    logging.info(amsg)
                    msg += amsg
                    continue

            storageSite = stageOut.get("storageSite")
            volume = stageOut.get("volume")
            protocol = stageOut.get("protocol")
            command = stageOut.get("command")
            rse = stageOut.get("rse")
            
            msg += "\nStage out to : %s using: %s" % (rse, command)
            
            try:
                aPath = storageJsonPath(self.siteCfg.siteName,self.siteCfg.subSiteName,storageSite)
                rfc = readRFC(aPath,storageSite,volume,protocol)
                self.stageOuts_rfcs.append((stageOut,rfc))
                msg += "\nRucio File Catalog has been loaded:"
                msg += "\n"+str(rfc)
            except Exception as ex:
                amsg = "\nUnable to load Rucio File Catalog. This stage out will not be attempted:\n"
                amsg += '\t'+stageOutStr(stageOut) + '\n'
                amsg += str(ex)
                msg += amsg
                logging.info(amsg)
                continue
        
        #no Rucio file catalog is initialized
        if not self.stageOuts_rfcs:
            raise StageOutInitError(msg)

        logging.info("==== Stageout configuration start ====")
        logging.info(msg)
        logging.info("==== Stageout configuration finish ====")

        return

    def initialiseOverride(self):
        """
        _initialiseOverride_

        Extract and verify that the Override parameters are all present

        """
        overrideConf = {
            "command": None,
            "option": None,
            "rse": None,
            "lfn-prefix": None,
        }
        try:
            overrideConf['command'] = self.overrideConf['command']
            overrideConf['rse'] = self.overrideConf['rse']
            overrideConf['lfn-prefix'] = self.overrideConf['lfn-prefix']
        except Exception as ex:
            msg = "Unable to extract override parameters from config:\n"
            msg += str(ex)
            raise StageOutInitError(msg)
        if 'option' in self.overrideConf and self.overrideConf['option'] is not None:
            if len(self.overrideConf['option']) > 0:
                overrideConf['option'] = self.overrideConf['option']
            else:
                overrideConf['option'] = ""

        self.overrideConf = overrideConf

        msg = "=======StageOut Override Initialised:================\n"
        for key, val in viewitems(self.overrideConf):
            msg += " %s : %s\n" % (key, val)
        msg += "=====================================================\n"
        logging.info(msg)
        return

    def __call__(self, fileToStage):
        """
        _operator()_

        Use call to invoke transfers

        """
        lastException = Exception("empty exception")

        logging.info("==>Working on file: %s", fileToStage['LFN'])
        lfn = fileToStage['LFN']

        fileToStage['StageOutReport'] = []
        
        # // No override => use stage-out from site conf
        if not self.override:
            logging.info("===> Attempting %s Stage Outs", len(self.stageOuts))
            for stageOut_rfc in self.stageOuts_rfcs:
                try:
                    pfn = self.stageOut(lfn, fileToStage['PFN'], fileToStage.get('Checksums'), stageOut_rfc)
                    fileToStage['PFN'] = pfn
                    fileToStage['RSE'] = stageOut_rfc[0]['rse']
                    fileToStage['StageOutCommand'] = stageOut_rfc[0]['command']
                    logging.info("attempting stageOut")
                    self.completedFiles[fileToStage['LFN']] = fileToStage
                    if lfn in self.failed:
                        del self.failed[lfn]

                    logging.info("===> Stage Out Successful: %s", fileToStage)
                    fileToStage = stageoutPolicyReport(fileToStage, None, None, 'LOCAL', 0)
                    return fileToStage
                except WMException as ex:
                    lastException = ex
                    logging.info("===> Stage Out Failure for file:")
                    logging.info("======>  %s\n", fileToStage['LFN'])
                    logging.info("======>  %s\n using this stage out", stageOutStr(stageOut_rfc[0]))
                    fileToStage = stageoutPolicyReport(fileToStage,stageOut_rfc[0]['rse'],
                                                      stageOut_rfc[0]['command'], 'LOCAL', 60311)
                    continue
                except Exception as ex:
                    lastException = StageOutFailure("Error during local stage out",
                                                    error=str(ex))
                    logging.info("===> Stage Out Failure for file:\n")
                    logging.info("======>  %s\n", fileToStage['LFN'])
                    logging.info("======>  %s\n using this stage out", stageOutStr(stageOut_rfc[0]))
                    fileToStage = stageoutPolicyReport(fileToStage, stageOut_rfc[0]['rse'],
                                                      stageOut_rfc[0]['command'], 'LOCAL', 60311)
                    continue
        
        else:
            logging.info("===> Attempting stage outs from override")
            try:
                pfn = self.stageOut(lfn, fileToStage['PFN'], fileToStage.get('Checksums'))
                fileToStage['PFN'] = pfn
                fileToStage['RSE'] = self.overrideConf['rse']
                fileToStage['StageOutCommand'] = self.overrideConf['command']
                logging.info("attempting override stage out")
                self.completedFiles[fileToStage['LFN']] = fileToStage
                if lfn in self.failed:
                    del self.failed[lfn]

                logging.info("===> Stage Out Successful: %s", fileToStage)
                fileToStage = stageoutPolicyReport(fileToStage, None, None, 'OVERRIDE', 0)
                return fileToStage
            except Exception as ex:
                fileToStage = stageoutPolicyReport(fileToStage, self.overrideConf['rse'],\
                    self.overrideConf['command'], 'OVERRIDE', 60310)
                lastException = ex

        raise lastException
    
    
    def stageOut(self, lfn, localPfn, checksums, stageOut_rfc=None):
        """
        _stageOut_

        Given the lfn and a pair of stage out and corresponding Rucio file catalog, stageOut_rfc, or override configuration invoke the stage out
        If use override configuration self.overrideConf should contain:
        command - the stage out impl plugin name to be used
        option - the option values to be passed to that command (None is allowed)
        lfn-prefix - the LFN prefix to generate the PFN
        rse - the Name of the Rucio storage element to which the file is being xferred
        """
        if not self.override:
            try:
                command = stageOut_rfc[0]['command']
                options = stageOut_rfc[0]['option']
            except Exception as ex:
                msg = "Unable to retrieve command and options for stage out\n"
                raise StageOutFailure(msg, LFN=lfn, ExceptionDetail=str(ex))

            pfn = getPFN(stageOut_rfc[1],lfn)
            protocol = stageOut_rfc[1].preferredProtocol
            if pfn == None:
                msg = "Unable to match lfn to pfn: \n  %s" % lfn
                raise StageOutFailure(msg, LFN=lfn, StageOut=stageOutStr(stageOut_rfc[0]))
            try:
                impl = retrieveStageOutImpl(command)
            except Exception as ex:
                msg = "Unable to retrieve impl for local stage out:\n"
                msg += "Error retrieving StageOutImpl for command named: %s\n" % (
                    command,)
                raise StageOutFailure(msg, Command=command,
                                      LFN=lfn, ExceptionDetail=str(ex))
            impl.numRetries = self.numberOfRetries
            impl.retryPause = self.retryPauseTime

            try:
                if not self.bypassImpl:
                    impl(protocol, localPfn, pfn, options, checksums)
            except Exception as ex:
                msg = "Failure for stage out:\n"
                msg += str(ex)
                try:
                    import traceback
                    msg += traceback.format_exc()
                except AttributeError as ex:
                    msg += "Traceback unavailable\n"
                raise StageOutFailure(msg, Command=command, Protocol=protocol,
                                      LFN=lfn, InputPFN=localPfn, TargetPFN=pfn)
            return pfn
      
        else:
          
            pfn = "%s%s" % (self.overrideConf['lfn-prefix'], lfn)

            try:
                impl = retrieveStageOutImpl(self.overrideConf['command'])
            except Exception as ex:
                msg = "Unable to retrieve impl for override stage out:\n"
                msg += "Error retrieving StageOutImpl for command named: "
                msg += "%s\n" % self.overrideConf['command']
                raise StageOutFailure(msg, Command=self.overrideConf['command'],
                                      LFN=lfn, ExceptionDetail=str(ex))

            impl.numRetries = self.numberOfRetries
            impl.retryPause = self.retryPauseTime

            try:
                if not self.bypassImpl:
                    impl(self.overrideConf['command'], localPfn, pfn, self.overrideConf["option"], checksums)
            except Exception as ex:
                msg = "Failure for override stage out:\n"
                msg += str(ex)
                raise StageOutFailure(msg, Command=self.overrideConf['command'],
                                      LFN=lfn, InputPFN=localPfn, TargetPFN=pfn)

            return pfn
    
    def cleanSuccessfulStageOuts(self):
        """
        _cleanSucessfulStageOuts_

        In the event of a failed stage out, this method can be called to cleanup the
        files that may have previously been staged out so that the job ends in a clear state
        of failure, rather than a partial success


        """
        for lfn, fileInfo in viewitems(self.completedFiles):
            pfn = fileInfo['PFN']
            command = fileInfo['StageOutCommand']
            msg = "Cleaning out file: %s\n" % lfn
            msg += "Removing PFN: %s" % pfn
            msg += "Using command implementation: %s\n" % command
            logging.info(msg)
            delManager = DeleteMgr(**self.overrideConf)
            delManager.bypassImpl = self.bypassImpl
            try:
                delManager.deletePFN(pfn, lfn, command)
            except StageOutFailure as ex:
                msg = "Failed to cleanup staged out file after error:"
                msg += " %s\n%s" % (lfn, str(ex))
                logging.error(msg)
