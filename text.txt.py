sudo iptables -t nat -A PREROUTING -p tcp --dport 8000 -j DNAT --to-destination 192.168.1.100:8000
sudo iptables -t nat -A POSTROUTING -p tcp -d 192.168.1.100 --dport 8000 -j MASQUERADE
