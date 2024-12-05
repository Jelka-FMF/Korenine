rm -f /tmp/jelka
mkfifo /tmp/jelka
chmod 0777 /tmp/jelka
python3 /home/jelka/Korenine/driver.py
