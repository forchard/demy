ansible-playbook -t memtotal monitor.yml 
cat monitor.mem.* | less -S
rm monitor.mem.* 
