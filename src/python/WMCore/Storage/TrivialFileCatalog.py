#!/usr/bin/env python
"""
_TrivialFileCatalog_

Object to contain LFN to PFN mappings from a Trivial File Catalog
and provide functionality to match LFNs against them

Usage:

given a TFC file, invoke readTFC on it. This will return
a TrivialFileCatalog instance that can be used to match LFNs
to PFNs.

Usage: Given a TFC contact string: trivialcatalog_file:/path?protocol=proto


    filename = tfcFilename(tfcContactString)
    protocol = tfcProtocol(tfcContactString)
    tfcInstance = readTFC(filename)

    lfn = "/store/PreProd/unmerged/somelfn.root"

    pfn = tfcInstance.matchLFN(protocol, lfn)


"""

from builtins import next, str, range
from future.utils import viewitems

from future import standard_library
standard_library.install_aliases()

import os
import re

import json

from urllib.parse import urlsplit
from xml.dom.minidom import Document

from WMCore.Algorithms.ParseXMLFile import xmlFileToNode

_TFCArgSplit = re.compile("\?protocol=")


class TrivialFileCatalog(dict):
    """
    _TrivialFileCatalog_

    Object that can map LFNs to PFNs based on contents of a Trivial
    File Catalog
    """

    def __init__(self):
        dict.__init__(self)
        self['lfn-to-pfn'] = []
        self['pfn-to-lfn'] = []
        self.preferredProtocol = None  # attribute for preferred protocol

    def addMapping(self, protocol, match, result,
                   chain=None, mapping_type='lfn-to-pfn'):
        """
        _addMapping_

        Add an lfn to pfn mapping to this instance

        """
        entry = {}
        entry.setdefault("protocol", protocol)
        entry.setdefault("path-match-expr", re.compile(match))
        entry.setdefault("path-match", match)
        entry.setdefault("result", result)
        entry.setdefault("chain", chain)
        self[mapping_type].append(entry)

    def _doMatch(self, protocol, path, style, caller):
        """
        Generalised way of building up the mappings.
        caller is the method from there this method was called, it's used
        for resolving chained rules

        Return None if no match

        """
        for mapping in self[style]:
            if mapping['protocol'] != protocol:
                continue
            if mapping['path-match-expr'].match(path) or mapping["chain"] != None:
                if mapping["chain"] != None:
                    oldpath = path
                    path = caller(mapping["chain"], path)
                    if not path:
                        continue
                splitList = []
                if len(mapping['path-match-expr'].split(path, 1)) > 1:
                    for split in range(len(mapping['path-match-expr'].split(path, 1))):
                        s = mapping['path-match-expr'].split(path, 1)[split]
                        if s:
                            splitList.append(s)
                else:
                    path = oldpath
                    continue
                result = mapping['result']
                for split in range(len(splitList)):
                    result = result.replace("$" + str(split + 1), splitList[split])
                return result

        return None

    def matchLFN(self, protocol, lfn):
        """
        _matchLFN_

        Return the result for the LFN provided if the LFN
        matches the path-match for that protocol

        Return None if no match

        """
        result = self._doMatch(protocol, lfn, "lfn-to-pfn", self.matchLFN)
        return result

    def matchPFN(self, protocol, pfn):
        """
        _matchLFN_

        Return the result for the LFN provided if the LFN
        matches the path-match for that protocol

        Return None if no match

        """
        result = self._doMatch(protocol, pfn, "pfn-to-lfn", self.matchPFN)
        return result

    def getXML(self):
        """
        Converts TFC implementation (dict) into a XML string representation.
        The method reflects this class implementation - dictionary containing
        list of mappings while each mapping (i.e. entry, see addMapping
        method) is a dictionary of key, value pairs.

        """

        def _getElementForMappingEntry(entry, mappingStyle):
            xmlDocTmp = Document()
            element = xmlDocTmp.createElement(mappingStyle)
            for k, v in viewitems(entry):
                # ignore empty, None or compiled regexp items into output
                if not v or (k == "path-match-expr"):
                    continue
                element.setAttribute(k, str(v))
            return element

        xmlDoc = Document()
        root = xmlDoc.createElement("storage-mapping")  # root element name
        for mappingStyle, mappings in viewitems(self):
            for mapping in mappings:
                mapElem = _getElementForMappingEntry(mapping, mappingStyle)
                root.appendChild(mapElem)
        return root.toprettyxml()

    def __str__(self):
        result = ""
        for mapping in ['lfn-to-pfn', 'pfn-to-lfn']:
            for item in self[mapping]:
                result += "\t%s: protocol=%s path-match-re=%s result=%s" % (
                    mapping,
                    item['protocol'],
                    item['path-match-expr'].pattern,
                    item['result'])
                if item['chain'] != None:
                    result += " chain=%s" % item['chain']
                result += "\n"
        return result

def tfcProtocol(contactString,useTFC):
    """
    _tfcProtocol_

    Given a file catalog contact string, extract the
    protocol from it. If useTFC=True, the Trivial File Catalog contact string is used (storage.xml or storage-disk.xml). Otherwise, it is the file catalog from Rucio definitions (storage.json) 

    """
    args = urlsplit(contactString)[3]
    value = args.replace("protocol=", '')
    if not useTFC:
        value = value.split('&')[0]
    return value

def getCatalogString(storageAttr):
    """
    Construct a catalog for storage.json from storage attributes. It is a string with the same format as old trivial catalog for storage.xml ('trivialcatalog_file:'+path+'?protocol='+storageAttr['protocol']+'&volume='+storageAttr['volume']. "path" points to location of storage.json, '/abc/xyz/storage.json')  
    :param storageAttr = {'site':siteName, 'subSite':subSiteName, 'storageSite':storageSiteName, 'volume':volume, 'protocol':protocol}
    :return catalogString('trivialcatalog_file:/pathToStorageDescription/xyz?protocol=protocolName&volume=volumeName')
    """
    site = storageAttr['site']
    subSite = storageAttr['subSite']
    storageSite = storageAttr['storageSite']
    #get site config
    siteConfigPath = os.getenv('SITECONFIG_PATH',None)
    if not siteConfigPath:
        raise RuntimeError('SITECONFIG_PATH is not defined')
    subPath = ''
    #not a cross site, use local path given in SITECONFIG_PATH
    if site == storageSite:
        #it is a site (no defined subSite), use local path given in SITECONFIG_PATH
        if subSite is None:
            subPath = siteConfigPath
        #it is a subsite, move one level up
        else:
            subPath = siteConfigPath + '/..'
    #cross site
    else:
        #it is a site (no defined subSite), move one level up
        if subSite is None:
            subPath = siteConfigPath + '/../' + storageSite
        #it is a subsite, move two levels up
        else:
            subPath = siteConfigPath + '/../../' + storageSite
    pathToStorageDescription = subPath + '/storage.json'
    pathToStorageDescription = os.path.normpath(os.path.realpath(pathToStorageDescription))#resolve symbolic link and relative path?
    catalogString = 'trivialcatalog_file:'+pathToStorageDescription+'?protocol='+storageAttr['protocol']+'&volume='+storageAttr['volume']
    return catalogString

#TFC file name can be constructed using either contactString or storage attributes
def tfcFilename(contactString):
    """
    _tfcFilename_

    Extract the filename from a TFC contact string.

    """
    value = contactString.replace("trivialcatalog_file:", "")
    value = _TFCArgSplit.split(value)[0]
    path = os.path.normpath(value) #does this resolve relative and symbolic path?
    return path

def readTFC(filename, storageAttr, useTFC):
    """
    _readTFC_

    Read the file provided and return a TrivialFileCatalog
    instance containing the details found in it

    """
    tfcInstance = TrivialFileCatalog()
    if useTFC:
        if not os.path.exists(filename):
            msg = "TrivialFileCatalog not found: %s" % filename
            raise RuntimeError(msg)
        try:
            node = xmlFileToNode(filename)
        except Exception as ex:
            msg = "Error reading TrivialFileCatalog: %s\n" % filename
            msg += str(ex)
            raise RuntimeError(msg)

        parsedResult = nodeReader(node)

      #tfcInstance = TrivialFileCatalog()
        for mapping in ['lfn-to-pfn', 'pfn-to-lfn']:
            for entry in parsedResult[mapping]:
                protocol = entry.get("protocol", None)
                match = entry.get("path-match", None)
                result = entry.get("result", None)
                chain = entry.get("chain", None)
                if True in (protocol, match == None):
                    continue
                tfcInstance.addMapping(str(protocol), str(match), str(result), chain, mapping)
      #return tfcInstance
    else:
        try:
            jsonFile = open(filename,encoding="utf-8")
            jsElements = json.load(jsonFile)
        except Exception as ex:
            msg = "Error reading FileCatalog: %s\n" % filename
            msg += str(ex)
            raise RuntimeError(msg)
        #now loop over elements, select the right one and fill lfn-to-pfn
        for jsElement in jsElements:
            if jsElement['site'] == storageAttr['site'] and jsElement['volume'] == storageAttr['volume']: 
                #now loop over protocols
                for proc in jsElement['protocols']:
                    #check found match
                    if proc['protocol'] == storageAttr['protocol']:
                        chain = proc['chain'] if 'chain' in proc.keys() else None
                        #check if prefix is in protocol block
                        if 'prefix' in proc.keys():
                            #lfn-to-pfn
                            match = '(.*)' #match all
                            result = proc['prefix']+'/$1'
                            tfcInstance.addMapping(str(proc['protocol']), str(match), str(result), chain, 'lfn-to-pfn')
                            #pfn-to-lfn
                            match = proc['prefix']+'/(.*)'
                            result = '/$1'
                            tfcInstance.addMapping(str(proc['protocol']), str(match), str(result), chain, 'pfn-to-lfn')
                        #here is rules  
                        else:
                            #loop over rules
                            for rule in proc['rules']:
                                match = rule['lfn']
                                result = rule['pfn']
                                tfcInstance.addMapping(str(proc['protocol']), str(match), str(result), chain, 'lfn-to-pfn')
                                #pfn-to-lfn: not sure about this!!!
                                match = rule['pfn'].replace('$1','(.*)')
                                result = rule['lfn'].replace('/+','/').replace('^/','/')
                                #now replace anything inside () with $1, for example (.*) --> $1, (store/.*) --> $1
                                result = re.sub('\(.*\)','$1',result)
                                tfcInstance.addMapping(str(proc['protocol']), str(match), str(result), chain, 'pfn-to-lfn')
    
    return tfcInstance

def rseName(storageAttr):
    rse = None
    catalog = getCatalogString(storageAttr)
    storageJsonName = tfcFilename(catalog)
    try:
        with open(storageJsonName,encoding="utf-8") as jsonFile:
            jsElements = json.load(jsonFile)
    except Exception as ex:
        msg = "TrivialFileCatalog.py:rseName() Error reading FileCatalog: %s\n" % storageJsonName 
        msg += str(ex)
        raise RuntimeError(msg)
    for jsElement in jsElements:
        if jsElement['site'] == storageAttr['storageSite'] and jsElement['volume'] == storageAttr['volume']:
            rse = jsElement['rse']
            break
    return rse

def lfnPrefix(storageAttr):
    catalog = getCatalogString(storageAttr)
    #now get lfn prefix
    storageJsonName = tfcFilename(catalog)
    try:
        with open(storageJsonName,encoding="utf-8") as jsonFile:
            jsElements = json.load(jsonFile)
    except Exception as ex:
        msg = "TrivialFileCatalog.py:lfnPrefix() Error reading FileCatalog: %s\n" % storageJsonName 
        msg += str(ex)
        raise RuntimeError(msg)
    #now loop over elements, select the right one and fill lfn-to-pfn
    lfnPrefixCol = []
    for jsElement in jsElements:
        if jsElement['site'] == storageAttr['storageSite'] and jsElement['volume'] == storageAttr['volume']: 
            #now loop over protocols
            for proc in jsElement['protocols']:
                #check found match
                if proc['protocol'] == storageAttr['protocol']:
                    #check if prefix in protocol block
                    if 'prefix' in proc.keys():
                        lfnPrefixCol.append(proc['prefix'])
                    #here is rules
                    else:
                        #loop over rules and keep all of them
                        for rule in proc['rules']:
                            lfnPrefixCol.append(rule['pfn'].replace('/$1','').replace('$1',''))
                    break #protocol found, do not need to proceed
            break
    return lfnPrefixCol

def loadTFC(contactString,storageAttr,useTFC):
    """
    _loadTFC_

    Given the contact string for the tfc, parse out the file location
    and the protocol and create a TFC instance

    """
    protocol = tfcProtocol(contactString,useTFC)
    tfcName = tfcFilename(contactString)
    instance = readTFC(tfcName,storageAttr,useTFC)
    instance.preferredProtocol = protocol
    return instance


def coroutine(func):
    """
    _coroutine_

    Decorator method used to prime coroutines

    """

    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr

    return start


def nodeReader(node):
    """
    _nodeReader_

    Given a node, see if we can find what we're looking for
    """

    processLfnPfn = {
        'path-match': processPathMatch(),
        'protocol': processProtocol(),
        'result': processResult(),
        'chain': processChain()
    }

    report = {'lfn-to-pfn': [], 'pfn-to-lfn': []}
    processSMT = processSMType(processLfnPfn)
    processor = expandPhEDExNode(processStorageMapping(processSMT))
    processor.send((report, node))
    return report


@coroutine
def expandPhEDExNode(target):
    """
    _expandPhEDExNode_

    If pulling a TFC from the PhEDEx DS, its wrapped in a top level <phedex> node,
    this routine handles that extra node if it exists
    """
    while True:
        report, node = (yield)
        sentPhedex = False
        for subnode in node.children:
            if subnode.name == "phedex":
                target.send((report, subnode))
                sentPhedex = True
        if not sentPhedex:
            target.send((report, node))


@coroutine
def processStorageMapping(target):
    """
    Process everything

    """
    while True:
        report, node = (yield)
        for subnode in node.children:
            if subnode.name == 'storage-mapping':
                target.send((report, subnode))


@coroutine
def processSMType(targets):
    """
    Process the type of storage-mapping

    """
    while True:
        report, node = (yield)
        for subnode in node.children:
            if subnode.name in ['lfn-to-pfn', 'pfn-to-lfn']:
                tmpReport = {'path-match-expr': subnode.name}
                targets['protocol'].send((tmpReport, subnode.attrs.get('protocol', None)))
                targets['path-match'].send((tmpReport, subnode.attrs.get('path-match', None)))
                targets['result'].send((tmpReport, subnode.attrs.get('result', None)))
                targets['chain'].send((tmpReport, subnode.attrs.get('chain', None)))
                report[subnode.name].append(tmpReport)


@coroutine
def processPathMatch():
    """
    Process path-match

    """
    while True:
        report, value = (yield)
        report['path-match'] = value


@coroutine
def processProtocol():
    """
    Process protocol

    """
    while True:
        report, value = (yield)
        report['protocol'] = value


@coroutine
def processResult():
    """
    Process result

    """
    while True:
        report, value = (yield)
        report['result'] = value


@coroutine
def processChain():
    """
    Process chain

    """
    while True:
        report, value = (yield)
        if value == "":
            report['chain'] = None
        else:
            report['chain'] = value
