ansible-playbook -t check_memory monitor.yml 
cat monitor.topmem.* | less -S
rm monitor.topmem.* 
