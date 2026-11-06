package jobs

import (
	"context"

	"github.com/bishopfox/sliver/client/console"
	"github.com/bishopfox/sliver/protobuf/clientpb"
	"github.com/spf13/cobra"
)

// TCPListenerCmd - Start a raw TCP listener
func TCPListenerCmd(cmd *cobra.Command, con *console.SliverClient, args []string) {
	lhost, _ := cmd.Flags().GetString("lhost")
	lport, _ := cmd.Flags().GetUint32("lport")

	con.PrintInfof("Starting TCP listener ...\n")

	tcp, err := con.Rpc.StartTCPListener(context.Background(), &clientpb.TCPListenerReq{
		Host: lhost,
		Port: lport,
	})

	con.Println()
	if err != nil {
		con.PrintErrorf("%s\n", err)
	} else {
		con.PrintInfof("Successfully started job #%d\n", tcp.JobID)
	}
}
