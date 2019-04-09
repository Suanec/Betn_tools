import xmltodict
print "\n".join([ '"' + k + '"' + ' -> ' + '"' + str(v) + '"' for (k,v) in xmltodict.parse(xml_input=open("./pipeline.xml","r").read()).get("configuration").get("nodes").get("node")[0].get("output").items() ])
