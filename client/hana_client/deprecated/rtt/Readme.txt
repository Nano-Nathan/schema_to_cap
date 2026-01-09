Repository Translation Tool (RTT)
*********************************

Requirements:
*************
Linux 32/64bit or Windows 32/64bit, Java 1.6.0+

Purpose:
********
Transport texts between NewDB (Repository) and the R3 translation system.

Installation:
*************
1. Install RTT with the NewDB client installation. RTT will be installed into "<hdbclient directory>/rtt".
The batch files "rtt.bat" and "rtt.sh" will be installed into "<hdbclient directory>".

2. Set the JAVA_HOME variable, e.g. for the Java JDK 1.8.0 on Windows, set it to
"C:\Program Files\Java\jdk1.8.0". Important: The Java platform (e.g. Win64) must match
the platform of the installed NewDB client installation.

3. Make sure that "<hdbclient directory>" is part of your search path ("PATH" environment variable).

4. Configure your NewDB and (optional) the R3 translation system in a properties file (e.g.
the default properties file "rtt.properties" in "<hdbclient directory>/rtt").

5. Open a shell and call rtt via "rtt.bat" or just "rtt" (Windows), or "rtt.sh" (Unix).

Usage:
******
-h,--help                Help 

--version                Version 

--export                 Export mode 

--upload                 Upload mode 

--download               Download mode 

--import                 Import mode 

-e,--exportUpload        Export and upload mode 

-i,--downloadImport      Download and import mode 

-p,--package             Packages to export/upload/download/import (default: no packages)

-d,-du,--deliveryUnit    Delivery units (format: <vendor>.<deliveryunit>) (default: no delivery units)

-l,--locale              Locales to download/import (default: all locales)

-c,--config              Configuration file (default: C:\Users\D050517\workspace-java-indigo\rtt.properties)

--skipReview             Upload: Specifies if the review step is to be skipped. WARNING: Setting the parameter to true will overwrite already reviewed texts. (default: false)

--languageChange         Upload: Allow original language change (default: false)

--noExcludePrivate       Export: Do not exclude objects marked as private (=''fncViewLayer'' tag set to ''Private'') (default: false)

-eu,--excludeUnassigned  Export: Specify suffixes of objects to exclude if their ''Category'' or ''Application Component'' is unassigned (=''fncViewLayer'' or ''fncAppCpt'' unset or empty) (default: no suffix)

-v,--verbose             Verbose mode (show messages with severity INFO in the console) (default: false)

-vv,--veryVerbose        Very verbose mode (show all messages with severity DEBUG in the console) (default: false)

-x,--xliffDir            XLIFF file directory (default: "rtt_exports" or "rtt_imports" in "C:\Users\D050517\AppData\Local\Temp\")

--severity               Log file severity level (all, debug, error, fatal, info, none, path or warning) (default: WARNING)

--force                  Force import of translated texts (skip source text matching) (default: false)

--noDelete               Keep XLIFF files from previous exports/downloads (default: false)

--dbReadFromStdin        Read database password from stdin (overrides config file and --dbpasswd) (default: false)

--r3ReadFromStdin        Read R3 translation system password from stdin (overrides config file and --r3passwd) (default: false)

--dbPasswd               Database password (overrides config file) (default: no password)

--r3Passwd               Database password (overrides config file) (default: no password)

-tan,--translationArea   Translation area (overrides jco.client.tan from the config file (default: jco.client.tan from the config file)

--simulate               Simulate write operations instead of executing them (upload, import) (default: false)

Examples:
*********
Basic
-----
rtt -e -p pack*
  Export the texts from those packages matching "pack*"
  from the database and upload the texts into the translation system,
  using the default configuration file "rtt.properties".
rtt -i -p pack*
  Download the translated texts from those packages matching "pack*"
  from the translation system and import the texts into the database,
  using the default configuration file "rtt.properties".

Export only
-----------
rtt --export -p pack* -x exports
  Export the texts from the database into the directory "exports".

Upload only
-----------
rtt --upload -p pack* -x exports --skipreview
  Upload the texts in the directory "exports" to the translation system,
  and skip the review step.

Download only
-------------
rtt --download -p pack* -x imports
  Download the translated texts into the directory "imports".

Import only
-----------
rtt --import -p pack* -x imports
  Import the translated texts from the directory "imports".

Delivery Units
--------------
rtt -e -du vendor1.du1
  Export and upload the texts from those packages
  contained in delivery unit "vendor1.du1".

rtt -e -p pack* -du vendor1.du1
  Export and upload the texts from those packages matching "pack*" or
  contained in delivery unit "vendor1.du1".

Languages
---------
rtt -i -p pack* -l de_DE -l fr_FR
  Download and import the texts translated into locales "de_DE" or "fr_FR".

Return codes:
*************
RTT returns error code 0 on success, 1 on error (exception), 2 if source text matching fails.

Passwords:
**********
The passwords for the HANA system and the R3 translation system can be provided either
1) in the configuration file (properties: "db.passwd" and "jco.client.passwd"),
2) as commandline arguments ("--dbPasswd" and "--r3Passwd"),
3) via standard input ("--dbReadFromStdin" and "--r3ReadFromStdin"),
4) (RTT 0.7.4+): via environment variables ("RTT_DBPASSWD" and "RTT_R3PASSWD"). 

Source text matching:
*********************
RTT does "source text matching" when it imports translated texts into the HANA database.
That means, it compares the original texts in the HANA database with the original texts
in the XLIFF files containing the translations (typically obtained from the R3 translation
system). 

If the original texts have changed, RTT does not import the translations!

This "source text matching" can be switched off/overridden using the "--force" switch.

Tags:
*****
You can specify the terminology domain of individual objects using the Repository tag
"hbtTerminologyDomain". This can be used to override the terminology domain specified
in the meta information of the package containing the object.

It can be set in the HANA Studio ("SAP HANA Studio Suite Extension View Properties" plug-in).

The terminology domain specified using the Repository tag "hbtTerminologyDomain" takes precedence
over the terminology domain specified using Xinfo objects (see below).

If the Repository tag "hbtEnforcePrivateTranslation" is set to "true", the corresponding object
is *not* excluded from the RTT operations (export, upload, download, import).

Else, if "hbtEnforcePrivateTranslation" is not set to "true" (or unset),
if the Repository tag "fncViewLayer" is set to "Private", the corresponding object is excluded
from the RTT operations (export, upload, download, import), unless you override this behavior 
(--noExcludePrivate).

If the option --excludeUnassigned is specified for a set of suffixes of objects, all objects
bearing these suffixes are excluded from the RTT operations if either the Repository tag
"fncViewLayer" or "fncAppCpt" is unset or empty.

Xinfo objects:
**************
You can override the text collection, terminology domain, responsible, hints for translation and
the text status of a package <PACKAGE> by creating "xinfo.json" objects in <PACKAGE>.

Here is an example:
{"object":[
  {"objects":[{"name":"ATTR1","type":"attributeview"},
              {"name":"ATTR2","type":"attributeview"}],
   "collection":"testna_collection1",
   "domain":"GR",
   "responsible":"reponsible1",
   "hints":"hints1",
   "status":"status1"},
  {"objects":[{"name":"ATTR3","type":"attributeview"}],
   "collection":"testna_collection2",
   "domain":"CO"}]}

In this case, the attribute view objects ATTR1 and ATTR2 are assigned to the text collection "testna_collection1" 
instead of the text collection defined for <PACKAGE>, the terminology domain "GR", the responsible "responsible1", 
the hints for translation "hints1" and the text status "status1".
 
The attribute view object ATTR3 is assigned to the text collection "testna_collection2" and the terminology domain 
"CO". The responsible, hints for translation and text status are taken from the respective definitions for 
<PACKAGE>.
 
All other objects in <PACKAGE> are assigned to the text collection, terminology domain, responsible,
hints for translation and text status defined for <PACKAGE>.

Example XLIFFs
**************

Non-translated (exported from HANA):

File name: 1.sap.hba.grc.AccessControlActionUsageSummaryQuery.calculationview.xlf 
--
<?xml version="1.0" encoding="UTF-8"?>
<xliff xmlns="urn:oasis:names:tc:xliff:document:1.2" version="1.2">
  <file datatype="plaintext" original="sap.hba.grc.AccessControlActionUsageSummaryQuery.calculationview" source-language="en">
    <header>
      <sxmd:metadata xmlns:sxmd="urn:x-sap:mlt:xliff12:metadata:1.0" xmlns="urn:x-sap:mlt:tsmetadata:1.0">
        <object-name>1.sap.hba.grc.AccessControlActionUsageSummaryQuery.calculationview</object-name>
        <collection>sap.hba.grc</collection>
        <domain>GR</domain>
        <developer>SYSTEM</developer>
        <description>AccessControlActionUsageSummary</description>
        <origin>sap.hba.grc.AccessControlActionUsageSummaryQuery.calculationview</origin>
      </sxmd:metadata>
    </header>
    <body>
      <group resname="o.AccessControlActionUsageSummaryQuery.calculationview" restype="x-objectTexts">
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="o.caption" maxwidth="255" sap:sc="XTIT" size-unit="char">
          <source>AccessControlActionUsageSummary</source>
        </trans-unit>
      </group>
      <group resname="c.AccessControlActionUsageSummaryQuery.calculationview" restype="x-objectContentTexts">
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="NumberOfExtActionExecution" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Number Of Ext Action Execution</source>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="LastExecutionDate" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Last Execution Date</source>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="ExecutionDateTime" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Execution Date Time</source>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="ActionDescription" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Action Description</source>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="ExtendedAction" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Extended Action</source>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="SystemConnection" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>System Connection</source>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="SAPClient" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>SAP Client</source>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="P_ExecutionEndDate" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Execution Date To</source>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="P_ExecutionStartDate" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Execution Date From</source>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="V_SAPClient" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>SAP Client</source>
        </trans-unit>
      </group>
    </body>
  </file>
</xliff>
--

Translated (downloaded from the translation system):

File name: 1.sap.hba.grc.AccessControlActionUsageSummaryQuery.calculationview-de.xlf

Translated XLIFF files must end with a dash followed by an ISO 639 language key (e.g. "-de")
or a combination of an ISO 639 language code, an underscore and an ISO 3166 country code (e.g. "-de_AT").

The "file" tag must include the attribute "target-language", which contains a combination of
an ISO 639 language key, a dash and an ISO 3166 country code (e.g. "de-DE").

Translated XLIFF files always contain the translation to precisely one target language.
--
<?xml version="1.0" encoding="UTF-8"?>
<xliff xmlns="urn:oasis:names:tc:xliff:document:1.2" version="1.2">
  <file datatype="plaintext" date="2013-12-05T08:27:26Z" original="sap.hba.grc.AccessControlActionUsageSummaryQuery.calculationview" source-language="en" target-language="de-DE">
    <header>
      <sxmd:metadata xmlns:sxmd="urn:x-sap:mlt:xliff12:metadata:1.0" xmlns="urn:x-sap:mlt:tsmetadata:1.0">
        <object-name>1.sap.hba.grc.AccessControlActionUsageSummaryQuery.calculationview</object-name>
        <collection>sap.hba.grc</collection>
        <domain>GR</domain>
        <developer>SYSTEM</developer>
        <description>AccessControlActionUsageSummary</description>
        <origin>sap.hba.grc.AccessControlActionUsageSummaryQuery.calculationview</origin>
      </sxmd:metadata>
    </header>
    <body>
      <group resname="o.AccessControlActionUsageSummaryQuery.calculationview" restype="x-objectTexts">
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="o.caption" maxwidth="255" sap:sc="XTIT" size-unit="char">
          <source>AccessControlActionUsageSummary</source>
          <target>Aktionsverwendungsübersicht in Access Control</target>
        </trans-unit>
      </group>
      <group resname="c.AccessControlActionUsageSummaryQuery.calculationview" restype="x-objectContentTexts">
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="V_SAPClient" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>SAP Client</source>
          <target>SAP-Mandant</target>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="P_ExecutionStartDate" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Execution Date From</source>
          <target>Ausführungsdatum von</target>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="P_ExecutionEndDate" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Execution Date To</source>
          <target>Ausführungsdatum bis</target>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="SAPClient" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>SAP Client</source>
          <target>SAP-Mandant</target>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="SystemConnection" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>System Connection</source>
          <target>Systemverbindung</target>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="ExtendedAction" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Extended Action</source>
          <target>Erweiterte Aktion</target>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="ActionDescription" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Action Description</source>
          <target>Aktionsbeschreibung</target>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="ExecutionDateTime" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Execution Date Time</source>
          <target>Ausführungsdatum/-uhrzeit</target>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="LastExecutionDate" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Last Execution Date</source>
          <target>Letztes Ausführungsdatum</target>
        </trans-unit>
        <trans-unit xmlns:sap="urn:x-sap:sls-mlt" id="NumberOfExtActionExecution" maxwidth="120" sap:sc="XCOL" size-unit="char">
          <source>Number Of Ext Action Execution</source>
          <target>AusführAnzahl erweit. Aktion</target>
        </trans-unit>
      </group>
    </body>
  </file>
</xliff>
--

Change Log:
***********
1.0.2 (2015-12-15)
* Fix for download/import (sh-HR => sr-CS => sh/Serbo-Croatian bug).

1.0.1 (2014-07-04)
* Added support for technical languages (e.g. en_US_saptrc = 1Q, en_US_sappsd = 2Q)
* Included newer mlt_base and mlt_tsapi libs from Nexus

1.0.0 (2014-07-01)
* Removed "--findobjects" and "--deleteobjects" (not related to the Repository - should be
provided by SLS).

0.7.7 (2014-06-10)
* Small fix for help (made it more clear that "-v"/"-vv" is for console output, and "--severity" for
log file output.
* Improved error messages for empty original language.
* Changed option --excludeUnassigned - not a boolean anymore, but allows to specify a set of suffixes
of objects to be excluded if their ''Category'' or ''Application Component'' is unassigned
(''fncViewLayer'' or ''fncAppCpt'' unset or empty) for Suite Live (VDM Plug-in).

0.7.6 (2014-05-14)
* Added support for VDM tag "hbtEnforcePrivateTranslation" to enforce export of objects marked as "Private"
(fncViewLayer==Private).

0.7.5 (2014-04-25)
* Fixes for help, exception handling.

0.7.4 (2014-03-24)
* Added support for providing the passwords via environment variables
("RTT_DBPASSWD" and "RTT_R3PASSWD").

0.7.3 (2014-03-17)
* Option --excludeUnassigned (default: false) to exclude objects with unassigned ''Category''
or ''Application Component'' (''fncViewLayer'' or ''fncAppCpt'' unset or empty) for Suite Live
(VDM Plug-in).
* More robust empty string recognition (trims white space).

0.7.2 (2013-12-26)
* Switched to libraries from Nexus (a bit newer).

0.7.1 (2013-12-19)
* Fix for download/import (no-NO => nb-NO => no/Norwegian (bokmal) bug).

0.7.0 (2013-12-12)
* Support for "simulation mode" (does not write anything to HANA/R3).
* (Not yet functional) support for technical language keys (1Q...).

0.6.4 (2013-11-14)
* Return code 2 if source text matching fails.
* More warnings instead of info messages.

0.6.3 (2013-11-14)
* Fix for findObjects and deleteObjects output (required verbose mode).
* Fix for findObjects and deleteObjects default for --objectName.
* Prints out connection details for HANA and R3 in verbose mode.

0.6.2 (2013-10-23)
* Yet another fix for the progress display.
* Added --objectname to search for translation system objects.

0.6.1 (2013-10-21)
* New option -tan/--translationArea to override the translation area from the settings file.

0.6.0 (2013-10-17)
* Added new functionality to find and delete outdated objects from the translation system.
Call
rtt --findobjects --collection COLLECTION --datefrom DATEFROM --dateto DATETO
to find all objects in the translation system in collection COLLECTION, whose last modification date
is between DATEFROM and DATETO.
Call
rtt --deleteobjects --collection COLLECTION --datefrom DATEFROM --dateto DATETO
to delete these objects.
* Less error-prone commandline parsing: Replaces "=" by spaces before parsing, such that e.g.
rtt --export -p=PACKAGE
is also supported alongside
rtt --export -p PACKAGE
* Added change log to readme file.

0.5.1 (2013-10-15)
* Better error handling for Repository communication (export/import) - RTT would not stop upon
Repository errors (e.g. errors caused by missing privileges upon import).

0.5.0 (2013-10-11)
* Critical fix for import - RTT would confuse texts.
* Better progress display.

0.4.2 (2013-09-30)
* Fix for responsible for xinfo.json.

0.4.1 (2013-09-19)
* Fix for private view exclusion.
* Improved output (progress indicator numbers).

0.4.0 (2013-07-04)
* Import now only imports specified languages
(or all if none specified), like download.
* Better description for the noDelete option.

0.3.5 (2013-07-03)
* Replaces all whitespace in package developer responsible and description with " "
to avoid errors/save time.

0.3.4 (2013-06-19)
* Removed string.empty() calls.

0.3.3 (2013-06-07)
* Info message if source text matching fails.
* Readme.txt section about source text matching.
* Better exception output.

0.3.2 (2013-03-26)
* Critical incompatible change: shorter text ids.

0.3.1 (2013-03-21)
* XLIFF description set to object text (view properties -> description).

0.3.0 (2013-03-18)
* Private tag now supported for all operations (not just export).
* Import of translations: json escaped.

0.2.1 (2013-03-15)
* Support for other tags fncAppCpt, fncTag, fncTreeTag, fncViewLayer
if fncViewLayer == "Private", do not export the corresponding object.

0.2.0 (2013-02-01)
* Added support for Repository tags. First tag: hbtTerminologyDomain.

0.1.1 (2013-01-22)
* Better error handling for vendor.du specifications.
* "ignoring text..." debug message for texts not marked for translation.
* Error code 1 for all exceptions (some would slip through and the
error code would be 0...).

0.1.0 (2012-12-21)
* Bug fix for JSON requests (package names not properly escaped).
* New veryVerbose mode (debug output).
* Return codes (0 for success, 1 for error).
