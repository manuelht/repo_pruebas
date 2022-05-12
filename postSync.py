import sys
import re

print("---------------------------------------")
print("--- POST SYNC FWK-TAPSFLOW PROJECT ----")
print("---------------------------------------")

#Variables de configuracion
allowed_diff="^[am|AM]"
allowed_folder_files="^properties/"
script_name = "validate_properties.py"

#Input Data
files_in=sys.argv
print("::Files_in - ")
print(files_in)

#Select files witch have 
files_out=[]
count=0
for item in files_in:
    if count > 0 and count%2!=0 and re.search(allowed_diff,item) and re.search(allowed_folder_files,files_in[count+1]):
        files_out.append(files_in[count+1])
    count+=1
print("::Files_out - " )    
print(files_out)


argv = []
for file in files_out:
    argv.append(script_name)
    argv.append(file.strip())

    script_descriptor = open(script_name)
    script_py = script_descriptor.read()
    sys.argv = argv
    exec(script_py)
    
    script_descriptor.close()
    argv.clear()

#----- END -------