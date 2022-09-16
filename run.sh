timeout 21600 /usr/bin/python /home/coffeyrt/311pull/311pull.py
/usr/bin/python /home/coffeyrt/311pull/311plot.py --database /media/main/Rory/311/311.db --out /media/main/Rory/311/
mv /media/main/Rory/311/311_preShattuck.png /media/main/Rory/311/franklin_pics/
mv /media/main/Rory/311/311_postShattuck.png /media/main/Rory/311/franklin_pics/
/usr/bin/python /home/coffeyrt/311pull/plot_trajectory.py --database /media/main/Rory/311/311.db --out /media/main/Rory/311/franklin_pics/
