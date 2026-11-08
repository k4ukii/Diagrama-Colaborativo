### Diagrama Colaborativo

Editor de diagramas colaborativo em tempo real com arquitetura cliente-servidor, sockets TCP/UDP e interface gráfica — implementado 100% com a biblioteca padrão do Python.

### Visão Geral

O Diagrama permite que múltiplos usuários desenhem simultaneamente no mesmo quadro digital interativo. Toda ação realizada por um participante é refletida imediatamente na tela dos demais conectados à sala.

Sem frameworks externos: Desenvolvido puramente com socket, threading, json e tkinter.

### Como Executar !!

1. Iniciar o Servidor

No terminal, inicie o servidor central:

python server.py --port 5000


(Por padrão, o servidor escuta conexões TCP na porta 5000 e abre um endpoint UDP na mesma porta).

2. Conectar os Clientes

Abra novos terminais para cada participante:

#### Cliente 1
python interface.py --host 127.0.0.1 --port 5000 --user Alice

#### Cliente 2
python interface.py --host 127.0.0.1 --port 5000 --user Bob

Em rede local: Troque 127.0.0.1 pelo IP do computador do servidor na rede e certifique-se de liberar as portas TCP e UDP no firewall.
