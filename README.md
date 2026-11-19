# shame_router

One HTTPS endpoint that routes colleagues' requests to idle vLLM / SGLang servers anyone runs on the m1 and m2 clusters.

## Gateway

1. Launch an Ubuntu 24.04 VM with a public IP, open to all inbound traffic (no firewall). On AWS:

   ```
   aws ec2 create-security-group --group-name shame-router --description "all inbound"
   aws ec2 authorize-security-group-ingress --group-name shame-router --protocol -1 --cidr 0.0.0.0/0
   AMI=$(aws ec2 describe-images --owners 099720109477 \
     --filters 'Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*' \
     --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text)
   aws ec2 run-instances --image-id "$AMI" --instance-type c7a.8xlarge --key-name <your-key> \
     --security-groups shame-router --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=24}'
   ```
2. On it: clone this repo, fill `.env` from `.env.example`, and run `sudo bash gateway/gateway.sh`.

## Clusters

On each cluster's login node, in a shell with `uv` on its PATH:

1. Clone this repo, and the tailscale_relay repo inside it:

   ```
   git clone <this repo> shame_router && cd shame_router
   git clone https://github.com/aldopareja/tailscale_relay.git tailscale_relay
   ```
2. Fill `.env` from `.env.example` (the same file as on the gateway), and link it for the relay:
   `ln -s ../.env tailscale_relay/.env`.
3. Write `opted_in.txt` in the repo root (git-ignored, like `.env`, since it names people): one cluster username per
   line, the users whose idle servers may be lent. It must exist before the scout starts: an empty file sweeps nothing,
   and a missing one stops every node's scout at its first round. A node is swept only while every job on it belongs
   to one of them. The scout reads the file every round, so edits apply within 5 min; servers already lent stay until
   their sidecars exit.
4. Check that the `#SBATCH` lines in `cluster/scout_m1.sbatch` or `cluster/scout_m2.sbatch` (partition, account, QOS,
   memory) fit your cluster, then submit from the repo root: `sbatch cluster/scout_m1.sbatch`.

Events go to `logs/<cluster>.jsonl`. To add or rename models, edit `cluster/model_names_normalized.tsv`; sidecars
read it again within 15 s. Stop with `scancel --name=shame-router-scout`.

## Clients

The gateway speaks the OpenAI API at `https://<gateway host>/v1`, with the gateway token as a bearer token. Ask for one
of the names `GET /v1/models` lists; it changes as owners' servers become idle or busy.

```
curl https://<gateway host>/v1/models -H "Authorization: Bearer <token>"
curl https://<gateway host>/v1/chat/completions -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{"model": "deepseek-v4.1-flash-1017", "messages": [{"role": "user", "content": "Hello"}]}'
```

With the OpenAI SDK: `OpenAI(base_url="https://<gateway host>/v1", api_key="<token>")`. Streaming works, a call can run
for up to 2 h, and a request without the token gets 403. A reply's `model` field is the server's own id for the model,
not the name asked for. `/v1/messages` works only where the server's engine supports it.
