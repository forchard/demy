ansible-playbook -t check_disk monitor.yml 
cat monitor.disk.* | less -S
rm monitor.disk.* 
