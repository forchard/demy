ansible-playbook -t check_memory monitor.yml 
cat monitor.out.* | less -S
rm monitor.out.* 
