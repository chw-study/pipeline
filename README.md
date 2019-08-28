# Pipeline

This executes a pipeline that consists of a shared portion and several outputs:

1. payments.py
2. reports.py
3. callcenter.py

Kubernetes deployment files are included in /kube and and example .env file with the necessary environment variables is included in /.example-env.

To run any of the code, you will need (3) excel files in AWS S3, the paths are currently hardcoded in the lib/utils.py file (in get_crosswalk, get_endline, and get_roster). Those files are not included in the repository for privacy reasons! You will also need a running Mongo database with the two collections: rawmessages, events.

callcenter.py will probably not be needed again, it picks messages to give to the callcenter workers.

payments.py and reports.py will write csv files to a hardcoded S3 path as well, which can be seen in the files.
