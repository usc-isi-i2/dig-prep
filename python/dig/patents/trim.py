#!/usr/bin/env python
import sys,os, re
try:
    from pyspark import SparkContext
except:
    print "No pyspark"
import getopt

test="""<PATDOC DTD="2.4" STATUS="BUILD 20010518">
<SDOBI>
<B100>
<B110><DNUM><PDAT>D0453058</PDAT></DNUM></B110>
<B130><PDAT>S1</PDAT></B130>
<B140><DATE><PDAT>20020129</PDAT></DATE></B140>
<B190><PDAT>US</PDAT></B190>
</B100>
<B200>
<B210><DNUM><PDAT>29124868</PDAT></DNUM></B210>
<B211US><PDAT>29</PDAT></B211US>
<B220><DATE><PDAT>20000613</PDAT></DATE></B220>
</B200>
<B400>
<B472>
<B474><PDAT>14</PDAT></B474>
</B472>
</B400>
<B500>
<B510>
<B511><PDAT>0201</PDAT></B511>
<B516><PDAT>7</PDAT></B516>
</B510>
<B520>
<B521><PDAT>D 2703</PDAT></B521>
<B522><PDAT> D2700</PDAT></B522>
<B522><PDAT> D2701</PDAT></B522>
<B522><PDAT> D2702</PDAT></B522>
<B522><PDAT> D2706</PDAT></B522>
</B520>
<B540><STEXT><PDAT>Corset</PDAT></STEXT></B540>
<B560>
<B561>
<PCIT>
<DOC><DNUM><PDAT>2131457</PDAT></DNUM>
<DATE><PDAT>19380900</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Tachat</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>450 41</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>2420575</PDAT></DNUM>
<DATE><PDAT>19470500</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Treadwell</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>450 39</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D164485</PDAT></DNUM>
<DATE><PDAT>19510900</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Fredericks</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2706</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D166760</PDAT></DNUM>
<DATE><PDAT>19520500</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>La Bue et al.</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2702</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D169510</PDAT></DNUM>
<DATE><PDAT>19530500</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>La Bue et al.</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2702</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D169511</PDAT></DNUM>
<DATE><PDAT>19530500</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>LaBue et al.</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2702</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D174114</PDAT></DNUM>
<DATE><PDAT>19550300</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Dior</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2702</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D174194</PDAT></DNUM>
<DATE><PDAT>19550300</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Prochaska</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2704</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D177067</PDAT></DNUM>
<DATE><PDAT>19560300</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Reisenfeld</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2702</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D177181</PDAT></DNUM>
<DATE><PDAT>19560300</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Kurzman</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2706</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D177982</PDAT></DNUM>
<DATE><PDAT>19560600</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Kahn</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2702</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>3090387</PDAT></DNUM>
<DATE><PDAT>19630500</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Hopper</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>450 74</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D206157</PDAT></DNUM>
<DATE><PDAT>19661100</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Maas</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2706</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D220741</PDAT></DNUM>
<DATE><PDAT>19710500</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Marcario et al.</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2707</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D265265</PDAT></DNUM>
<DATE><PDAT>19820700</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Stern et al.</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2710</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D276001</PDAT></DNUM>
<DATE><PDAT>19841000</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Locascio</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2710</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B561>
<PCIT>
<DOC><DNUM><PDAT>D373236</PDAT></DNUM>
<DATE><PDAT>19960900</PDAT></DATE></DOC>
<PARTY-US>
<NAM><SNM><STEXT><PDAT>Vass-Betts</PDAT></STEXT></SNM></NAM>
</PARTY-US>
<PNC><PDAT>D 2710</PDAT></PNC></PCIT><CITED-BY-EXAMINER>
</B561>
<B562><NCIT><STEXT><PDAT>Frederick&apos;s of Hollywood; p. 37&mdash;Lace corset on upper right corner of page, (Item F).* </PDAT></STEXT></NCIT><CITED-BY-OTHER></B562>
<B562><NCIT><STEXT><PDAT>Frederick&apos;s of Hollywood; p. 47&mdash;black corset on lower right corner of page, (Item K).* </PDAT></STEXT></NCIT><CITED-BY-OTHER></B562>
<B562><NCIT><STEXT><PDAT>Frederick&apos;s of Hollywood; p. 49&mdash;White lace corset, Item F. (middle of page).</PDAT></STEXT></NCIT><CITED-BY-EXAMINER></B562>
</B560>
<B570>
<B577><PDAT>1</PDAT></B577>
<B578US><PDAT>1</PDAT></B578US>
</B570>
<B580>
<B583US><PDAT>D 2700-714</PDAT></B583US>
<B582><PDAT>D 2738</PDAT></B582>
<B582><PDAT>D24190</PDAT></B582>
<B582><PDAT>D24124</PDAT></B582>
<B582><PDAT>D24126</PDAT></B582>
<B582><PDAT>  2 44</PDAT></B582>
<B582><PDAT>  2 48</PDAT></B582>
<B582><PDAT>  2 67</PDAT></B582>
<B582><PDAT>  2 782</PDAT></B582>
<B582><PDAT>  2 783</PDAT></B582>
<B582><PDAT>  2227</PDAT></B582>
<B582><PDAT>  2228</PDAT></B582>
<B582><PDAT>  2238</PDAT></B582>
<B582><PDAT>  2400</PDAT></B582>
<B582><PDAT>  2402</PDAT></B582>
<B582><PDAT>  2403</PDAT></B582>
<B582><PDAT>  2406</PDAT></B582>
<B582><PDAT>  2408</PDAT></B582>
<B582><PDAT>  2DIG 9</PDAT></B582>
<B582><PDAT>  2300</PDAT></B582>
<B582><PDAT>  2302</PDAT></B582>
<B583US><PDAT>450  1-  5</PDAT></B583US>
<B582><PDAT>450 13</PDAT></B582>
<B582><PDAT>450 30</PDAT></B582>
<B582><PDAT>450 32</PDAT></B582>
<B582><PDAT>450 39</PDAT></B582>
<B582><PDAT>450 41</PDAT></B582>
<B582><PDAT>450 43</PDAT></B582>
<B582><PDAT>450 47</PDAT></B582>
<B582><PDAT>450 48</PDAT></B582>
<B582><PDAT>450 54</PDAT></B582>
<B582><PDAT>450 57</PDAT></B582>
<B582><PDAT>450 58</PDAT></B582>
<B582><PDAT>450 74</PDAT></B582>
<B582><PDAT>450 83</PDAT></B582>
<B582><PDAT>450 86</PDAT></B582>
<B582><PDAT>450 89</PDAT></B582>
<B582><PDAT>450104</PDAT></B582>
<B582><PDAT>450109</PDAT></B582>
<B582><PDAT>450116</PDAT></B582>
</B580>
<B590><B595><PDAT>5</PDAT></B595><B596><PDAT>7</PDAT></B596><B597US>
</B590>
</B500>
<B600>
<B630><B632><PARENT-US><CDOC><DOC><DNUM><PDAT>29/124868</PDAT></DNUM></DOC></CDOC><PDOC><DOC><DNUM><PDAT>29/107998</PDAT></DNUM><DATE><PDAT>19990720</PDAT></DATE><CTRY><PDAT>US</PDAT></CTRY><KIND><PDAT>00</PDAT></KIND></DOC></PDOC><PSTA><PDAT>03</PDAT></PSTA></PARENT-US></B632></B630>
</B600>
<B700>
<B720>
<B721>
<PARTY-US>
<NAM><FNM><PDAT>Park</PDAT></FNM><SNM><STEXT><PDAT>Kim</PDAT></STEXT></SNM></NAM>
<ADR>
<STR><PDAT>33-102 Hyosung Villa, 63 Chungdam-dong, Kangnam-ku</PDAT></STR>
<CITY><PDAT>Seoul</PDAT></CITY>
<CTRY><PDAT>KR</PDAT></CTRY>
</ADR>
</PARTY-US>
</B721>
</B720>
<B740>
<B741>
<PARTY-US>
<NAM><ONM><STEXT><PDAT>Jacobson Holman, PLLC</PDAT></STEXT></ONM></NAM>
</PARTY-US>
</B741>
</B740>
<B745>
<B746>
<PARTY-US>
<NAM><FNM><PDAT>Celia</PDAT></FNM><SNM><STEXT><PDAT>Murphy</PDAT></STEXT></SNM></NAM>
</PARTY-US>
</B746>
<B748US><PDAT>2914</PDAT></B748US>
</B745>
</B700>
</SDOBI>
<SDODE>
<DRWDESC>
<BTEXT>
<PARA ID="P-00001" LVL="7"><PTEXT><PDAT>FIG. 1 is a perspective view of a corset showing my new design;</PDAT></PTEXT></PARA>
<PARA ID="P-00002" LVL="7"><PTEXT><PDAT>FIG. 2 is a bottom plan view thereof;</PDAT></PTEXT></PARA>
<PARA ID="P-00003" LVL="7"><PTEXT><PDAT>FIG. 3 is a top plan view thereof;</PDAT></PTEXT></PARA>
<PARA ID="P-00004" LVL="7"><PTEXT><PDAT>FIG. 4 is a front elevational view thereof;</PDAT></PTEXT></PARA>
<PARA ID="P-00005" LVL="7"><PTEXT><PDAT>FIG. 5 is a rear elevational view thereof;</PDAT></PTEXT></PARA>
<PARA ID="P-00006" LVL="7"><PTEXT><PDAT>FIG. 6 is a left side view thereof; and,</PDAT></PTEXT></PARA>
<PARA ID="P-00007" LVL="7"><PTEXT><PDAT>FIG. 7 is a right side view thereof.</PDAT></PTEXT></PARA>
<PARA ID="P-00008" LVL="7"><PTEXT><PDAT>The broken lines shown throughout the views are understood to represent stitching.</PDAT></PTEXT></PARA>
</BTEXT>
</DRWDESC>
</SDODE>
<SDOCL>
<CL>
<CLM ID="CLM-00001">
<PARA ID="P-00009" LVL="7"><PTEXT><PDAT>The ornamental design for a corset, as shown and described.</PDAT></PTEXT></PARA>
</CLM>
</CL>
</SDOCL>
<SDODR ID="DRAWINGS">
<EMI ID="EMI-D00000" FILE="USD0453058-20020129-D00000.TIF">
<EMI ID="EMI-D00001" FILE="USD0453058-20020129-D00001.TIF">
<EMI ID="EMI-D00002" FILE="USD0453058-20020129-D00002.TIF">
<EMI ID="EMI-D00003" FILE="USD0453058-20020129-D00003.TIF">
<EMI ID="EMI-D00004" FILE="USD0453058-20020129-D00004.TIF">
<EMI ID="EMI-D00005" FILE="USD0453058-20020129-D00005.TIF">
</SDODR>
</PATDOC>"""

test2="""<PARA ID="P-00002" LVL="0"><PTEXT><PDAT>This <DEL-S DATE="20020129" ID="DEL-S-00001">application<DEL-E ID="DEL-S-00001">  <INS-S DATE="20020129" ID="INS-S-00001">reissue application </PDAT><HIL><BOLD><PDAT>09</PDAT></BOLD></HIL><PDAT>/</PDAT><HIL><BOLD><PDAT>500</PDAT></BOLD></HIL><PDAT>,</PDAT><HIL><BOLD><PDAT>252</PDAT></BOLD></HIL><PDAT>, is a continuation of co-</PDAT><HIL><ITALIC><PDAT>pending Reissue application Ser. No. </PDAT><HIL><BOLD><PDAT>09</PDAT></BOLD></HIL><PDAT>/</PDAT><HIL><BOLD><PDAT>164</PDAT></BOLD></HIL><PDAT>,</PDAT><HIL><BOLD><PDAT>985</PDAT></BOLD></HIL><PDAT>, filed Oct. </PDAT><HIL><BOLD><PDAT>1</PDAT></BOLD></HIL><PDAT>, </PDAT><HIL><BOLD><PDAT>1998</PDAT></BOLD></HIL><PDAT> both of which are reissues of U.S. Pat. Nos. </PDAT><HIL><BOLD><PDAT>5</PDAT></BOLD></HIL><PDAT>,</PDAT><HIL><BOLD><PDAT>689</PDAT></BOLD></HIL><PDAT>,</PDAT><HIL><BOLD><PDAT>891</PDAT></BOLD></HIL></ITALIC></HIL><PDAT>(</PDAT><HIL><ITALIC><PDAT>filed May </PDAT><HIL><BOLD><PDAT>30</PDAT></BOLD></HIL><PDAT>, </PDAT><HIL><BOLD><PDAT>1996</PDAT></BOLD></HIL><PDAT> as Ser. No. </PDAT><HIL><BOLD><PDAT>08</PDAT></BOLD></HIL><PDAT>/</PDAT><HIL><BOLD><PDAT>658</PDAT></BOLD></HIL><PDAT>,</PDAT><HIL><BOLD><PDAT>889</PDAT></BOLD></HIL></ITALIC></HIL><PDAT>) </PDAT><HIL><ITALIC><PDAT>which </PDAT></ITALIC></HIL><PDAT><INS-E ID="INS-S-00001">is a continuation-in-part of U.S. patent applications Ser. No. 08/444,069, now U.S. Pat. No. 5,566,458, and Ser. No. 08/443,784, now U.S. Pat. No. 5,607,023, both filed May 18, 1995, and both of which are continuation-in-part applications of U.S. patent applications Ser. No. 08/354,518, abandoned, and Ser. No. 08/354,560, abandoned, both filed Dec. 13, 1994.</PDAT></PTEXT></PARA>"""


def trim(tup):
    payload = tup[1].encode("utf-8")
    # first get rid of large sections we don't care about, including substructure
    # There should be two terminating conditions
    # (i) an optional space followed by key=value followed by > + minimal enclosed content followed by close tag
    # (ii) plain />
    p1 = re.compile(r"""<((SDODR|SDOCL|SDODE|B600|B300)(:? ?[^>]*>.*?</\2>)|/>)""", flags=re.S)
    replacement = "M"*100
    replacement = ""
    payload = re.sub(p1, replacement, payload)
    # medium-sized sections we don't care about which sometimes have broken internal structure (e.g. MATHML)
    p2 = re.compile(r"""<((CWU|MATHML|DETDESC|DRAWDESC|MATH-US|math)(:? ?[^>]*>.*?</\2>)|/>)""", flags=re.S)
    payload = re.sub(p2, replacement, payload)
    # these are tags, possibly with parameters which are often improperly closed
    # so we allow matching open and close tags with anything up to close bracket
    p3 = re.compile(r"""<(/?(EMI|COLSPEC|INS-S|INS-E|DEL-S|DEL-E|CHEMMOL|CHEMCDX|MATHEMATICA|CUSTOM-CHARACTER)[^>]*>)""", flags=re.S)
    payload = re.sub(p3, replacement, payload)
    # these are empty tags which are often improperly expressed as bare open tags
    payload = payload.replace('<CITED-BY-EXAMINER>',replacement).replace('<CITED-BY-OTHER>',replacement).replace('<B597US>',replacement).replace('<B473US>',replacement).replace('<B221US>',replacement)
    # internal to ML/mathematica
    payload = payload.replace('<none>',replacement)
    return (tup[0], payload)

def main(argv):
    inputSequenceDir = ""
    outputSequenceDir = ""
    try:
        opts, args = getopt.getopt(argv,"i:o:")
    except getopt.GetoptError:
        sys.exit(2)
    for (opt,arg) in opts :
        if opt == '-i':
            inputSequenceDir = arg
        elif opt == '-o' :
            outputSequenceDir = arg
    sc = SparkContext(appName="Fix XML App")
    datarawRDD = sc.sequenceFile(inputSequenceDir)
    cleanedRDD = datarawRDD.map(lambda x : trim(x))
    
    outputFormatClassName = "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"
    conf1= {"mapreduce.output.fileoutputformat.compress": "true", 
            "mapreduce.output.fileoutputformat.compress.codec":"org.apache.hadoop.io.compress.DefaultCodec",
            "mapreduce.output.fileoutputformat.compress.type":"RECORD"}
    cleanedRDD.saveAsNewAPIHadoopFile(outputSequenceDir,outputFormatClassName,"org.apache.hadoop.io.Text","org.apache.hadoop.io.Text",None,None,conf1)

    print "OK Bye Bye"

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
