# Exit immediately if a command exits with a non-zero status
set -e

echo "Please be careful with using sudo."

sudo cp -r /opt/mobilecoach-server/mc_global/ /opt/backups/mc_global_"$(date +"%d-%m-%Y")"

echo "Backup done"
