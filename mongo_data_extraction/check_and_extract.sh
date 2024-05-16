#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Ask the user about the backup
read -p "Have you taken a backup of the /opt/mobilecoach-server folder? (y/n): " yn
case $yn in
    [Yy]* ) echo "Proceeding with the script...";;  # Proceed if the answer is yes
    [Nn]* ) echo "Please take a backup before proceeding. Exiting..."; exit 1;;  # Exit if the answer is no
    * ) echo "Invalid input. Please answer yes or no. Exiting..."; exit 1;;  # Exit on invalid input
esac

echo "Please be careful with using sudo."

# Create a folder for the data up to the date
EXPORT_DIR="mongo_export_$(date +"%d-%m-%Y")"
echo "Creating export folder: $EXPORT_DIR"
mkdir "$EXPORT_DIR"

# Move into the export directory
cd "$EXPORT_DIR"

echo "The data will be exported to $(pwd) directory"

# Please confirm using "docker container ls" command if mongodb container name is mobilecoach-server-mongodbservice-1. If not, change it in the below commands.
CONTAINER_NAME="mobilecoach-server-mongodbservice-1"

# Run mongoexport commands within the Docker container
sudo docker container exec "$CONTAINER_NAME" mongoexport --db=mc --collection=InterventionVariableWithValue --type=csv --fields=_id,name,value,intervention,privacyType,accessType --out=/InterventionVariableWithValue.csv
sudo docker container exec "$CONTAINER_NAME" mongoexport --db=mc --collection=ParticipantVariableWithValue --type=csv --fields=_id,name,value,participant,timestamp,describesMediaUpload,formerVariableValues --out=/ParticipantVariableWithValue.csv
sudo docker container exec "$CONTAINER_NAME" mongoexport --db=mc --collection=DialogMessage --type=csv --fields=_id,participant,order,pushOnly,status,type,clientId,message,messageWithForcedLinks,textFormat,shouldBeSentTimestamp,sentTimestamp,supervisorMessage,messageExpectsAnswer,answerCanBeCancelled,messageIsSticky,messageDeactivatesAllOpenQuestions,isUnansweredAfterTimestamp,answerReceivedTimestamp,answerReceived,answerNotAutomaticallyProcessable,mediaContentViewed,manuallySent --out=/DialogMessage.csv
sudo docker container exec "$CONTAINER_NAME" mongoexport --db=mc --collection=Participant --type=csv --fields=_id,intervention,createdTimestamp,lastLoginTimestamp,lastLogoutTimestamp,nickname,language,group,monitoringActive,organization,organizationUnit,lastContactPassiveSensingTimestamp,missingPermissions --out=/Participant.csv
sudo docker container exec "$CONTAINER_NAME" mongoexport --db=mc --collection=DialogOption --type=csv --fields=_id,participant,type,data,pushNotificationTokens --out=/DialogOption.csv
sudo docker container exec "$CONTAINER_NAME" mongoexport --db=mc --collection=Intervention --type=csv --fields=_id,name,created,active,monitoringActive,dashboardEnabled,dashboardTemplatePath,deepstreamPassword,automaticallyFinishScreeningSurveys,interventionsToCheckForUniqueness,monitoringStartingDays --out=/Intervention.csv

# Copy the exported files from the Docker container to the host
sudo docker container cp "$CONTAINER_NAME":/InterventionVariableWithValue.csv .
sudo docker container cp "$CONTAINER_NAME":/ParticipantVariableWithValue.csv .
sudo docker container cp "$CONTAINER_NAME":/DialogMessage.csv .
sudo docker container cp "$CONTAINER_NAME":/Participant.csv .
sudo docker container cp "$CONTAINER_NAME":/DialogOption.csv .
sudo docker container cp "$CONTAINER_NAME":/Intervention.csv .

echo "Mongo data export has finished"

# Go back to the script folder
cd ..

echo "Script completed successfully."
