package replication

import "fmt"

type Sender struct {
	client          *replicationServiceClient
	replicationChan <-chan *ReplicationUnit
	done            <-chan struct{}
}

func (sender *Sender) SendReplicationUnits() {

	for {

		select {

		case replicationUnit := <-sender.replicationChan:

			_, err := sender.client.SendReplicationUnit(nil, replicationUnit)

			if err != nil {
				fmt.Print("do something here")
			}

		}
	}
}
