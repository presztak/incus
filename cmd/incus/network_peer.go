package main

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	cli "github.com/lxc/incus/v6/internal/cmd"
	"github.com/lxc/incus/v6/internal/i18n"
	"github.com/lxc/incus/v6/shared/api"
	"github.com/lxc/incus/v6/shared/termios"
)

type cmdNetworkPeer struct {
	global *cmdGlobal
}

// Command returns a cobra.Command for use with (*cobra.Command).AddCommand.
func (c *cmdNetworkPeer) Command() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = usage("peer")
	cmd.Short = i18n.G("Manage network peerings")
	cmd.Long = cli.FormatSection(i18n.G("Description"), i18n.G("Manage network peerings"))

	// List.
	networkPeerListCmd := cmdNetworkPeerList{global: c.global, networkPeer: c}
	cmd.AddCommand(networkPeerListCmd.Command())

	// Show.
	networkPeerShowCmd := cmdNetworkPeerShow{global: c.global, networkPeer: c}
	cmd.AddCommand(networkPeerShowCmd.Command())

	// Create.
	networkPeerCreateCmd := cmdNetworkPeerCreate{global: c.global, networkPeer: c}
	cmd.AddCommand(networkPeerCreateCmd.Command())

	// Get,
	networkPeerGetCmd := cmdNetworkPeerGet{global: c.global, networkPeer: c}
	cmd.AddCommand(networkPeerGetCmd.Command())

	// Set.
	networkPeerSetCmd := cmdNetworkPeerSet{global: c.global, networkPeer: c}
	cmd.AddCommand(networkPeerSetCmd.Command())

	// Unset.
	networkPeerUnsetCmd := cmdNetworkPeerUnset{global: c.global, networkPeer: c, networkPeerSet: &networkPeerSetCmd}
	cmd.AddCommand(networkPeerUnsetCmd.Command())

	// Edit.
	networkPeerEditCmd := cmdNetworkPeerEdit{global: c.global, networkPeer: c}
	cmd.AddCommand(networkPeerEditCmd.Command())

	// Delete.
	networkPeerDeleteCmd := cmdNetworkPeerDelete{global: c.global, networkPeer: c}
	cmd.AddCommand(networkPeerDeleteCmd.Command())

	// Workaround for subcommand usage errors. See: https://github.com/spf13/cobra/issues/706
	cmd.Args = cobra.NoArgs
	cmd.Run = func(cmd *cobra.Command, _ []string) { _ = cmd.Usage() }
	return cmd
}

// List.
type cmdNetworkPeerList struct {
	global      *cmdGlobal
	networkPeer *cmdNetworkPeer

	flagFormat  string
	flagColumns string
}

type networkPeerColumn struct {
	Name string
	Data func(api.NetworkPeer) string
}

// Command returns a cobra.Command for use with (*cobra.Command).AddCommand.
func (c *cmdNetworkPeerList) Command() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = usage("list", i18n.G("[<remote>:]<network>"))
	cmd.Aliases = []string{"ls"}
	cmd.Short = i18n.G("List available network peers")
	cmd.Long = cli.FormatSection(i18n.G("Description"), i18n.G(
		`List available network peers

Default column layout: ndpts

== Columns ==
The -c option takes a comma separated list of arguments that control
which network zone attributes to output when displaying in table or csv
format.

Column arguments are either pre-defined shorthand chars (see below),
or (extended) config keys.

Commas between consecutive shorthand chars are optional.

Pre-defined column shorthand chars:
  n - Name
  d - description
  p - Peer
  t - Type
  s - State`))

	cmd.RunE = c.Run
	cmd.Flags().StringVarP(&c.flagFormat, "format", "f", c.global.defaultListFormat(), i18n.G(`Format (csv|json|table|yaml|compact|markdown), use suffix ",noheader" to disable headers and ",header" to enable it if missing, e.g. csv,header`)+"``")
	cmd.Flags().StringVarP(&c.flagColumns, "columns", "c", defaultNetworkPeerListColumns, i18n.G("Columns")+"``")

	cmd.PreRunE = func(cmd *cobra.Command, _ []string) error {
		return cli.ValidateFlagFormatForListOutput(cmd.Flag("format").Value.String())
	}

	cmd.ValidArgsFunction = func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return c.global.cmpNetworks(toComplete)
		}

		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	return cmd
}

const defaultNetworkPeerListColumns = "ndpts"

func (c *cmdNetworkPeerList) parseColumns() ([]networkPeerColumn, error) {
	columnsShorthandMap := map[rune]networkPeerColumn{
		'n': {i18n.G("NAME"), c.nameColumnData},
		'd': {i18n.G("DESCRIPTION"), c.descriptionColumnData},
		'p': {i18n.G("PEER"), c.peerColumnData},
		't': {i18n.G("TYPE"), c.typeColumnData},
		's': {i18n.G("STATE"), c.stateColumnData},
	}

	columnList := strings.Split(c.flagColumns, ",")
	columns := []networkPeerColumn{}

	for _, columnEntry := range columnList {
		if columnEntry == "" {
			return nil, fmt.Errorf(i18n.G("Empty column entry (redundant, leading or trailing command) in '%s'"), c.flagColumns)
		}

		for _, columnRune := range columnEntry {
			column, ok := columnsShorthandMap[columnRune]
			if !ok {
				return nil, fmt.Errorf(i18n.G("Unknown column shorthand char '%c' in '%s'"), columnRune, columnEntry)
			}

			columns = append(columns, column)
		}
	}

	return columns, nil
}

func (c *cmdNetworkPeerList) nameColumnData(peer api.NetworkPeer) string {
	return peer.Name
}

func (c *cmdNetworkPeerList) descriptionColumnData(peer api.NetworkPeer) string {
	return peer.Description
}

func (c *cmdNetworkPeerList) peerColumnData(peer api.NetworkPeer) string {
	target := "Unknown"

	if peer.TargetProject != "" && peer.TargetNetwork != "" {
		target = fmt.Sprintf("%s/%s", peer.TargetProject, peer.TargetNetwork)
	} else if peer.TargetIntegration != "" {
		target = peer.TargetIntegration
	}

	return target
}

func (c *cmdNetworkPeerList) typeColumnData(peer api.NetworkPeer) string {
	return peer.Type
}

func (c *cmdNetworkPeerList) stateColumnData(peer api.NetworkPeer) string {
	return strings.ToUpper(peer.Status)
}

// Run runs the actual command logic.
func (c *cmdNetworkPeerList) Run(cmd *cobra.Command, args []string) error {
	// Quick checks.
	exit, err := c.global.checkArgs(cmd, args, 1, 1)
	if exit {
		return err
	}

	// Parse remote.
	remote := ""
	if len(args) > 0 {
		remote = args[0]
	}

	resources, err := c.global.parseServers(remote)
	if err != nil {
		return err
	}

	resource := resources[0]

	if resource.name == "" {
		return errors.New(i18n.G("Missing network name"))
	}

	peers, err := resource.server.GetNetworkPeers(resource.name)
	if err != nil {
		return err
	}

	// Parse column flags.
	columns, err := c.parseColumns()
	if err != nil {
		return err
	}

	data := [][]string{}
	for _, peer := range peers {
		line := []string{}
		for _, column := range columns {
			line = append(line, column.Data(peer))
		}

		data = append(data, line)
	}

	sort.Sort(cli.SortColumnsNaturally(data))

	header := []string{}
	for _, column := range columns {
		header = append(header, column.Name)
	}

	return cli.RenderTable(os.Stdout, c.flagFormat, header, data, peers)
}

// Show.
type cmdNetworkPeerShow struct {
	global      *cmdGlobal
	networkPeer *cmdNetworkPeer
}

// Command returns a cobra.Command for use with (*cobra.Command).AddCommand.
func (c *cmdNetworkPeerShow) Command() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = usage("show", i18n.G("[<remote>:]<network> <peer name>"))
	cmd.Short = i18n.G("Show network peer configurations")
	cmd.Long = cli.FormatSection(i18n.G("Description"), i18n.G("Show network peer configurations"))
	cmd.RunE = c.Run

	cmd.ValidArgsFunction = func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return c.global.cmpNetworks(toComplete)
		}

		if len(args) == 1 {
			return c.global.cmpNetworkPeers(args[0])
		}

		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	return cmd
}

// Run runs the actual command logic.
func (c *cmdNetworkPeerShow) Run(cmd *cobra.Command, args []string) error {
	// Quick checks.
	exit, err := c.global.checkArgs(cmd, args, 2, 2)
	if exit {
		return err
	}

	// Parse remote.
	resources, err := c.global.parseServers(args[0])
	if err != nil {
		return err
	}

	resource := resources[0]

	if resource.name == "" {
		return errors.New(i18n.G("Missing network name"))
	}

	if args[1] == "" {
		return errors.New(i18n.G("Missing peer name"))
	}

	client := resource.server

	// Show the network peer config.
	peer, _, err := client.GetNetworkPeer(resource.name, args[1])
	if err != nil {
		return err
	}

	data, err := yaml.Marshal(&peer)
	if err != nil {
		return err
	}

	fmt.Printf("%s", data)

	return nil
}

// Create.
type cmdNetworkPeerCreate struct {
	global      *cmdGlobal
	networkPeer *cmdNetworkPeer

	flagType        string
	flagDescription string
}

// Command returns a cobra.Command for use with (*cobra.Command).AddCommand.
func (c *cmdNetworkPeerCreate) Command() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = usage("create", i18n.G("[<remote>:]<network> <peer_name> <[target project/]<target network or integration> [key=value...]"))
	cmd.Aliases = []string{"add"}
	cmd.Short = i18n.G("Create new network peering")
	cmd.Long = cli.FormatSection(i18n.G("Description"), i18n.G("Create new network peering"))
	cmd.Example = cli.FormatSection("", i18n.G(`incus network peer create default peer1 web/default
    Create a new peering between network "default" in the current project and network "default" in the "web" project

incus network peer create default peer2 ovn-ic --type=remote
    Create a new peering between network "default" in the current project and other remote networks through the "ovn-ic" integration

incus network peer create default peer3 web/default < config.yaml
	Create a new peering between network default in the current project and network default in the web project using the configuration
	in the file config.yaml`))

	cmd.RunE = c.Run

	cmd.Flags().StringVar(&c.flagType, "type", "local", i18n.G("Type of peer (local or remote)")+"``")
	cmd.Flags().StringVar(&c.flagDescription, "description", "", i18n.G("Peer description")+"``")

	cmd.ValidArgsFunction = func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return c.global.cmpNetworks(toComplete)
		}

		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	return cmd
}

// Run runs the actual command logic.
func (c *cmdNetworkPeerCreate) Run(cmd *cobra.Command, args []string) error {
	// Quick checks.
	exit, err := c.global.checkArgs(cmd, args, 3, -1)
	if exit {
		return err
	}

	if !slices.Contains([]string{"local", "remote"}, c.flagType) {
		return errors.New(i18n.G("Invalid peer type"))
	}

	// Parse remote.
	resources, err := c.global.parseServers(args[0])
	if err != nil {
		return err
	}

	resource := resources[0]

	if resource.name == "" {
		return errors.New(i18n.G("Missing network name"))
	}

	if args[1] == "" {
		return errors.New(i18n.G("Missing peer name"))
	}

	if args[2] == "" {
		return errors.New(i18n.G("Missing target network or integration"))
	}

	targetParts := strings.SplitN(args[2], "/", 2)

	var targetProject, target string
	if len(targetParts) == 2 {
		targetProject = targetParts[0]
		target = targetParts[1]
	} else {
		target = targetParts[0]
	}

	// If stdin isn't a terminal, read yaml from it.
	var peerPut api.NetworkPeerPut
	if !termios.IsTerminal(getStdinFd()) {
		contents, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}

		err = yaml.UnmarshalStrict(contents, &peerPut)
		if err != nil {
			return err
		}
	}

	if peerPut.Config == nil {
		peerPut.Config = map[string]string{}
	}

	// Get config filters from arguments.
	for i := 3; i < len(args); i++ {
		entry := strings.SplitN(args[i], "=", 2)
		if len(entry) < 2 {
			return fmt.Errorf(i18n.G("Bad key/value pair: %s"), args[i])
		}

		peerPut.Config[entry[0]] = entry[1]
	}

	// Create the network peer.
	peer := api.NetworkPeersPost{
		Name:           args[1],
		NetworkPeerPut: peerPut,
		Type:           c.flagType,
	}

	switch c.flagType {
	case "local":
		peer.TargetProject = targetProject
		peer.TargetNetwork = target
	case "remote":
		peer.TargetIntegration = target
	}

	if c.flagDescription != "" {
		peer.Description = c.flagDescription
	}

	client := resource.server

	err = client.CreateNetworkPeer(resource.name, peer)
	if err != nil {
		return err
	}

	if !c.global.flagQuiet {
		createdPeer, _, err := client.GetNetworkPeer(resource.name, peer.Name)
		if err != nil {
			return fmt.Errorf(i18n.G("Failed getting peer's status: %w"), err)
		}

		switch createdPeer.Status {
		case api.NetworkStatusCreated:
			fmt.Printf(i18n.G("Network peer %s created")+"\n", peer.Name)
		case api.NetworkStatusPending:
			fmt.Printf(i18n.G("Network peer %s pending (please complete mutual peering on peer network)")+"\n", peer.Name)
		default:
			fmt.Printf(i18n.G("Network peer %s is in unexpected state %q")+"\n", peer.Name, createdPeer.Status)
		}
	}

	return nil
}

// Get.
type cmdNetworkPeerGet struct {
	global      *cmdGlobal
	networkPeer *cmdNetworkPeer

	flagIsProperty bool
}

// Command returns a cobra.Command for use with (*cobra.Command).AddCommand.
func (c *cmdNetworkPeerGet) Command() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = usage("get", i18n.G("[<remote>:]<network> <peer_name> <key>"))
	cmd.Short = i18n.G("Get values for network peer configuration keys")
	cmd.Long = cli.FormatSection(i18n.G("Description"), i18n.G("Get values for network peer configuration keys"))
	cmd.RunE = c.Run

	cmd.Flags().BoolVarP(&c.flagIsProperty, "property", "p", false, i18n.G("Get the key as a network peer property"))

	cmd.ValidArgsFunction = func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return c.global.cmpNetworks(toComplete)
		}

		if len(args) == 1 {
			return c.global.cmpNetworkPeers(args[0])
		}

		if len(args) == 2 {
			return c.global.cmpNetworkPeerConfigs(args[0], args[1])
		}

		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	return cmd
}

// Run runs the actual command logic.
func (c *cmdNetworkPeerGet) Run(cmd *cobra.Command, args []string) error {
	// Quick checks.
	exit, err := c.global.checkArgs(cmd, args, 3, 3)
	if exit {
		return err
	}

	// Parse remote
	resources, err := c.global.parseServers(args[0])
	if err != nil {
		return err
	}

	resource := resources[0]
	client := resource.server

	if resource.name == "" {
		return errors.New(i18n.G("Missing network name"))
	}

	if args[1] == "" {
		return errors.New(i18n.G("Missing peer name"))
	}

	// Get the current config.
	peer, _, err := client.GetNetworkPeer(resource.name, args[1])
	if err != nil {
		return err
	}

	if c.flagIsProperty {
		w := peer.Writable()
		res, err := getFieldByJSONTag(&w, args[2])
		if err != nil {
			return fmt.Errorf(i18n.G("The property %q does not exist on the network peer %q: %v"), args[2], resource.name, err)
		}

		fmt.Printf("%v\n", res)
	} else {
		for k, v := range peer.Config {
			if k == args[2] {
				fmt.Printf("%s\n", v)
			}
		}
	}

	return nil
}

// Set.
type cmdNetworkPeerSet struct {
	global      *cmdGlobal
	networkPeer *cmdNetworkPeer

	flagIsProperty bool
}

// Command returns a cobra.Command for use with (*cobra.Command).AddCommand.
func (c *cmdNetworkPeerSet) Command() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = usage("set", i18n.G("[<remote>:]<network> <peer_name> <key>=<value>..."))
	cmd.Short = i18n.G("Set network peer keys")
	cmd.Long = cli.FormatSection(i18n.G("Description"), i18n.G(
		`Set network peer keys

For backward compatibility, a single configuration key may still be set with:
    incus network set [<remote>:]<network> <peer_name> <key> <value>`))
	cmd.RunE = c.Run

	cmd.Flags().BoolVarP(&c.flagIsProperty, "property", "p", false, i18n.G("Set the key as a network peer property"))

	cmd.ValidArgsFunction = func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return c.global.cmpNetworks(toComplete)
		}

		if len(args) == 1 {
			return c.global.cmpNetworkPeers(args[0])
		}

		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	return cmd
}

// Run runs the actual command logic.
func (c *cmdNetworkPeerSet) Run(cmd *cobra.Command, args []string) error {
	// Quick checks.
	exit, err := c.global.checkArgs(cmd, args, 3, -1)
	if exit {
		return err
	}

	// Parse remote.
	resources, err := c.global.parseServers(args[0])
	if err != nil {
		return err
	}

	resource := resources[0]

	if resource.name == "" {
		return errors.New(i18n.G("Missing network name"))
	}

	if args[1] == "" {
		return errors.New(i18n.G("Missing peer name"))
	}

	client := resource.server

	// Get the current config.
	peer, etag, err := client.GetNetworkPeer(resource.name, args[1])
	if err != nil {
		return err
	}

	if peer.Config == nil {
		peer.Config = map[string]string{}
	}

	// Set the keys.
	keys, err := getConfig(args[2:]...)
	if err != nil {
		return err
	}

	writable := peer.Writable()
	if c.flagIsProperty {
		if cmd.Name() == "unset" {
			for k := range keys {
				err := unsetFieldByJSONTag(&writable, k)
				if err != nil {
					return fmt.Errorf(i18n.G("Error unsetting property: %v"), err)
				}
			}
		} else {
			err := unpackKVToWritable(&writable, keys)
			if err != nil {
				return fmt.Errorf(i18n.G("Error setting properties: %v"), err)
			}
		}
	} else {
		maps.Copy(writable.Config, keys)
	}

	return client.UpdateNetworkPeer(resource.name, peer.Name, writable, etag)
}

// Unset.
type cmdNetworkPeerUnset struct {
	global         *cmdGlobal
	networkPeer    *cmdNetworkPeer
	networkPeerSet *cmdNetworkPeerSet

	flagIsProperty bool
}

// Command returns a cobra.Command for use with (*cobra.Command).AddCommand.
func (c *cmdNetworkPeerUnset) Command() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = usage("unset", i18n.G("[<remote>:]<network> <peer_name> <key>"))
	cmd.Short = i18n.G("Unset network peer configuration keys")
	cmd.Long = cli.FormatSection(i18n.G("Description"), i18n.G("Unset network peer keys"))
	cmd.RunE = c.Run

	cmd.Flags().BoolVarP(&c.flagIsProperty, "property", "p", false, i18n.G("Unset the key as a network peer property"))

	cmd.ValidArgsFunction = func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return c.global.cmpNetworks(toComplete)
		}

		if len(args) == 1 {
			return c.global.cmpNetworkPeers(args[0])
		}

		if len(args) == 2 {
			return c.global.cmpNetworkPeerConfigs(args[0], args[1])
		}

		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	return cmd
}

// Run runs the actual command logic.
func (c *cmdNetworkPeerUnset) Run(cmd *cobra.Command, args []string) error {
	// Quick checks.
	exit, err := c.global.checkArgs(cmd, args, 3, 3)
	if exit {
		return err
	}

	c.networkPeerSet.flagIsProperty = c.flagIsProperty

	args = append(args, "")
	return c.networkPeerSet.Run(cmd, args)
}

// Edit.
type cmdNetworkPeerEdit struct {
	global      *cmdGlobal
	networkPeer *cmdNetworkPeer
}

// Command returns a cobra.Command for use with (*cobra.Command).AddCommand.
func (c *cmdNetworkPeerEdit) Command() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = usage("edit", i18n.G("[<remote>:]<network> <peer_name>"))
	cmd.Short = i18n.G("Edit network peer configurations as YAML")
	cmd.Long = cli.FormatSection(i18n.G("Description"), i18n.G("Edit network peer configurations as YAML"))
	cmd.RunE = c.Run

	cmd.ValidArgsFunction = func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return c.global.cmpNetworks(toComplete)
		}

		if len(args) == 1 {
			return c.global.cmpNetworkPeers(args[0])
		}

		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	return cmd
}

func (c *cmdNetworkPeerEdit) helpTemplate() string {
	return i18n.G(
		`### This is a YAML representation of the network peer.
### Any line starting with a '# will be ignored.
###
### An example would look like:
### description: A peering to mynet
### config: {}
### name: mypeer
### target_project: default
### target_network: mynet
### status: Pending
###
### Note that the name, target_project, target_network and status fields cannot be changed.`)
}

// Run runs the actual command logic.
func (c *cmdNetworkPeerEdit) Run(cmd *cobra.Command, args []string) error {
	// Quick checks.
	exit, err := c.global.checkArgs(cmd, args, 2, 2)
	if exit {
		return err
	}

	// Parse remote.
	resources, err := c.global.parseServers(args[0])
	if err != nil {
		return err
	}

	resource := resources[0]

	if resource.name == "" {
		return errors.New(i18n.G("Missing network name"))
	}

	if args[1] == "" {
		return errors.New(i18n.G("Missing peer name"))
	}

	client := resource.server

	// If stdin isn't a terminal, read text from it
	if !termios.IsTerminal(getStdinFd()) {
		contents, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}

		// Allow output of `incus network peer show` command to be passed in here, but only take the contents
		// of the NetworkPeerPut fields when updating. The other fields are silently discarded.
		newData := api.NetworkPeer{}
		err = yaml.UnmarshalStrict(contents, &newData)
		if err != nil {
			return err
		}

		return client.UpdateNetworkPeer(resource.name, args[1], newData.NetworkPeerPut, "")
	}

	// Get the current config.
	peer, etag, err := client.GetNetworkPeer(resource.name, args[1])
	if err != nil {
		return err
	}

	data, err := yaml.Marshal(&peer)
	if err != nil {
		return err
	}

	// Spawn the editor.
	content, err := textEditor("", []byte(c.helpTemplate()+"\n\n"+string(data)))
	if err != nil {
		return err
	}

	for {
		// Parse the text received from the editor.
		newData := api.NetworkPeer{} // We show the full info, but only send the writable fields.
		err = yaml.UnmarshalStrict(content, &newData)
		if err == nil {
			err = client.UpdateNetworkPeer(resource.name, args[1], newData.Writable(), etag)
		}

		// Respawn the editor.
		if err != nil {
			fmt.Fprintf(os.Stderr, i18n.G("Config parsing error: %s")+"\n", err)
			fmt.Println(i18n.G("Press enter to open the editor again or ctrl+c to abort change"))

			_, err := os.Stdin.Read(make([]byte, 1))
			if err != nil {
				return err
			}

			content, err = textEditor("", content)
			if err != nil {
				return err
			}

			continue
		}

		break
	}

	return nil
}

// Delete.
type cmdNetworkPeerDelete struct {
	global      *cmdGlobal
	networkPeer *cmdNetworkPeer
}

// Command returns a cobra.Command for use with (*cobra.Command).AddCommand.
func (c *cmdNetworkPeerDelete) Command() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.Use = usage("delete", i18n.G("[<remote>:]<network> <peer_name>"))
	cmd.Aliases = []string{"rm", "remove"}
	cmd.Short = i18n.G("Delete network peerings")
	cmd.Long = cli.FormatSection(i18n.G("Description"), i18n.G("Delete network peerings"))
	cmd.RunE = c.Run

	cmd.ValidArgsFunction = func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return c.global.cmpNetworks(toComplete)
		}

		if len(args) == 1 {
			return c.global.cmpNetworkPeers(args[0])
		}

		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	return cmd
}

// Run runs the actual command logic.
func (c *cmdNetworkPeerDelete) Run(cmd *cobra.Command, args []string) error {
	// Quick checks.
	exit, err := c.global.checkArgs(cmd, args, 2, 2)
	if exit {
		return err
	}

	// Parse remote.
	resources, err := c.global.parseServers(args[0])
	if err != nil {
		return err
	}

	resource := resources[0]

	if resource.name == "" {
		return errors.New(i18n.G("Missing network name"))
	}

	if args[1] == "" {
		return errors.New(i18n.G("Missing peer name"))
	}

	client := resource.server

	// Delete the network peer.
	err = client.DeleteNetworkPeer(resource.name, args[1])
	if err != nil {
		return err
	}

	if !c.global.flagQuiet {
		fmt.Printf(i18n.G("Network peer %s deleted")+"\n", args[1])
	}

	return nil
}
