To run the code:
1. Compile the files by running the file "compilefiles.sh"
2. Export the classpath using: export CLASSPATH=../externaljars/log4j-1.2.17.jar:.
3. Run the code using: java StartMemListDaemon "introducerhostname"

Logfiles are stored in the file logfile.log in the same folder as the class files.

It should show you the following options
1. X: On entering X, the localmachine leaves the group voluntarily
2. D: Will display the current membershiplist once
3. C: Will display the membershiplist periodically every 1s
4. S: Will stop displaying the membership list