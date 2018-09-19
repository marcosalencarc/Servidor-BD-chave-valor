// Implementation of a KeyValueServer. Students should write their code in this file.

package p1
import(
	"net"
	"strconv"
	"bufio"
	//"io"
	"strings"
	//"fmt"
	//"fmt"
	//"fmt"
    "time"
)
type Cliente struct {
	conexao net.Conn
	canalEncerrarCliente chan int
	bufferCliente chan []byte
}
type Dado struct{
	chave string
	valor []byte
}

type keyValueServer struct {
	listener				net.Listener
	listaClientes          []*Cliente
	canalPut               chan *Dado
	canalGet               chan *Dado

	canalAddClientes       chan *Cliente
	canalMatarCliente      chan *Cliente
	qntClientes            chan int
	canalEncerrarGoRoutine chan bool
	canalContarClientes    chan bool
	// TODO: implement this!
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	server:= &keyValueServer{
		listener:nil,
		listaClientes:          nil,
		canalPut:               make(chan *Dado),
		canalGet:               make(chan *Dado),
		canalAddClientes:       make(chan *Cliente),
		canalMatarCliente:      make(chan *Cliente),
		canalEncerrarGoRoutine: make(chan bool),
		qntClientes:            make(chan int),
		canalContarClientes:    make(chan bool)}
	return server
}

func (kvs *keyValueServer) Start(port int) error {

	porta := strconv.Itoa(port)
	lis, err := net.Listen("tcp", "localhost:"+ porta)
	if err != nil {
		return err
	}
	init_db()
	kvs.listener = lis
	go manipulaClientes(kvs)
	go gerenciaConec(kvs)

	return nil
}
func (kvs *keyValueServer) Close() {
	kvs.listener.Close()

}

func (kvs *keyValueServer) Count() int {
	kvs.canalContarClientes <-true
	return <-kvs.qntClientes
}
func manipulaClientes(kvs *keyValueServer){
	//fmt.Println("-----Manipulando clientes-------")

	for{
		select{

			case cliente :=<-kvs.canalAddClientes:
				kvs.listaClientes = append(kvs.listaClientes, cliente)
				go manusearConec(kvs,cliente)
				go func() {
					for {
						select {
						case <-cliente.canalEncerrarCliente:
							return
						case msg := <-cliente.bufferCliente:
							cliente.conexao.Write(msg)
						}
					}
				}()

			case dadoCanal := <-kvs.canalPut:
				put(dadoCanal.chave, dadoCanal.valor)

			case dao := <-kvs.canalGet:
				key := dao.chave
				valor := append(append([]byte(key),","...),get(dao.chave)...)
				menssagem := append(valor,[]byte("\n")...)
				broadcast(kvs,menssagem)

			case clienteMorto := <-kvs.canalMatarCliente:
				//fmt.Println("Matei um cliente")
				for i, c := range kvs.listaClientes {
					if c == clienteMorto {
						//fmt.Println("Aqui")
						c.conexao.Close()
						kvs.listaClientes =	append(kvs.listaClientes[:i], kvs.listaClientes[i+1:]...)
						break
					}
				}
				//fmt.Println("Encerrei")
			case <-kvs.canalContarClientes:
				kvs.qntClientes  <-len(kvs.listaClientes)


		}

	}
}

func gerenciaConec (kvs *keyValueServer){
	for {
		select {
		case <-kvs.canalEncerrarGoRoutine:
			return
		default:
			for {
				conn, err := kvs.listener.Accept()
				if err == nil {
					cliente := Cliente{conn, make(chan int), make(chan []byte,500)}
					kvs.canalAddClientes <- &cliente

				}
			}
		}
	}
}


func manusearConec(kvs *keyValueServer,cliente *Cliente){

	buf := bufio.NewReader(cliente.conexao)

	for {
		select {
		case <-cliente.canalEncerrarCliente:
			return
		default:
				menssagem, err := buf.ReadString('\n')
				menssage := strings.Replace(menssagem,"\n","",1)
				if err != nil {
					kvs.canalMatarCliente <-cliente
					return
				}else {
					msg := strings.Split(menssage, ",")
					if string(msg[0]) == "put" {
						d := Dado{msg[1], []byte(msg[2])}
						kvs.canalPut <- &d
					} else {
						d2 := Dado{chave: msg[1]}
						kvs.canalGet <- &d2
					}
				}
		}
	}
}



func broadcast(kvs *keyValueServer,msg []byte){
	//fmt.Println(string(msg))
	for _, c := range kvs.listaClientes {
		if len(c.bufferCliente) == 500 {
			<-c.bufferCliente
			time.Sleep(1)
		}
		c.bufferCliente <- msg
	}

}






