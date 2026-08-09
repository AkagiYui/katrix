package csapi

import (
	"encoding/json"
	"net/http"

	"github.com/AkagiYui/katrix/internal/appservice"
	"github.com/AkagiYui/katrix/internal/httpx"
)

// Third-party network metadata proxying (spec §Third-party networks): the
// homeserver exposes the appservices' third-party protocols to clients via
// /thirdparty/protocols, /thirdparty/protocol/{protocol}, and the user/location
// lookup endpoints, proxying each query to every appservice that declares the
// protocol and merging the responses. Merged protocol metadata is cached
// briefly (tpMeta), mirroring Synapse's AS response cache: a client asking for
// one protocol right after the merged listing (sytest "HS can provide query
// metadata on a single protocol") must see the previously fetched metadata even
// if the AS now answers an empty stub.

// ThirdPartyProtocols handles GET /_matrix/client/v3/thirdparty/protocols.
// It fetches the protocol metadata from every appservice and returns a map of
// protocol name -> metadata (sytest "HS provides query metadata" asserts the
// instances from both ASes are merged).
func (a *API) ThirdPartyProtocols(w http.ResponseWriter, r *http.Request) {
	if a.HS.AppServices == nil {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{})
		return
	}
	client := appservice.NewClient(a.Config.FederationInsecure)
	out := map[string]any{}
	protocols := map[string]map[string]any{}
	for _, reg := range a.HS.AppServices.All() {
		for _, proto := range reg.Protocols {
			meta, ok := a.tpMetaProtocol(r, client, reg, proto)
			if !ok {
				continue
			}
			var obj map[string]any
			if json.Unmarshal(meta, &obj) != nil {
				continue
			}
			// Merge instances lists from all ASes declaring the protocol
			// (sytest expects all three instances across the two ASes).
			existing := protocols[proto]
			if existing == nil {
				existing = map[string]any{}
				for k, v := range obj {
					if k != "instances" {
						existing[k] = v
					}
				}
				protocols[proto] = existing
			}
			if instances, ok := obj["instances"].([]any); ok {
				cur, _ := existing["instances"].([]any)
				existing["instances"] = append(cur, instances...)
			}
		}
	}
	for proto, meta := range protocols {
		out[proto] = meta
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

// tpMetaProtocol returns an AS's protocol metadata for `proto`, consulting the
// metadata cache first and refreshing it from the AS on a miss.
func (a *API) tpMetaProtocol(r *http.Request, client *appservice.Client, reg *appservice.Registration, proto string) (json.RawMessage, bool) {
	key := proto + "\x00" + reg.URL
	if cached := a.tpMeta.get(key); cached != nil {
		return cached, true
	}
	meta, ok := client.ProtocolMetadata(r.Context(), reg, proto)
	if !ok {
		return nil, false
	}
	a.tpMeta.set(key, meta)
	return meta, true
}

// ThirdPartyProtocol handles GET /_matrix/client/v3/thirdparty/protocol/{protocol}.
// It fetches the protocol metadata from every appservice that declares it and
// merges the result (sytest "HS can provide query metadata on a single protocol").
func (a *API) ThirdPartyProtocol(w http.ResponseWriter, r *http.Request) {
	if a.HS.AppServices == nil {
		httpx.WriteError(w, httpx.ErrNotFound("protocol not found"))
		return
	}
	protocol := r.PathValue("protocol")
	client := appservice.NewClient(a.Config.FederationInsecure)
	var merged map[string]any
	have := false
	for _, reg := range a.HS.AppServices.All() {
		declared := false
		for _, p := range reg.Protocols {
			if p == protocol {
				declared = true
				break
			}
		}
		if !declared {
			continue
		}
		meta, ok := a.tpMetaProtocol(r, client, reg, protocol)
		if !ok {
			continue
		}
		var obj map[string]any
		if json.Unmarshal(meta, &obj) != nil {
			continue
		}
		have = true
		if merged == nil {
			merged = map[string]any{}
			for k, v := range obj {
				if k != "instances" {
					merged[k] = v
				}
			}
			merged["instances"] = []any{}
		}
		if instances, ok := obj["instances"].([]any); ok {
			cur, _ := merged["instances"].([]any)
			merged["instances"] = append(cur, instances...)
		}
	}
	if !have || merged == nil {
		httpx.WriteError(w, httpx.ErrNotFound("protocol not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, merged)
}

// ThirdPartyUser handles GET /_matrix/client/v3/thirdparty/user/{protocol}.
// It proxies the lookup (with the client's search fields as query parameters)
// to every appservice that declares the protocol and concatenates the results
// (sytest "HS will proxy request for 3PU mapping").
func (a *API) ThirdPartyUser(w http.ResponseWriter, r *http.Request) {
	a.thirdPartyLookup(w, r, true)
}

// ThirdPartyLocation handles GET /_matrix/client/v3/thirdparty/location/{protocol}.
func (a *API) ThirdPartyLocation(w http.ResponseWriter, r *http.Request) {
	a.thirdPartyLookup(w, r, false)
}

func (a *API) thirdPartyLookup(w http.ResponseWriter, r *http.Request, users bool) {
	if a.HS.AppServices == nil {
		httpx.WriteJSON(w, http.StatusOK, []any{})
		return
	}
	protocol := r.PathValue("protocol")
	fields := map[string]string{}
	for k := range r.URL.Query() {
		if k == "access_token" {
			continue
		}
		fields[k] = r.URL.Query().Get(k)
	}
	client := appservice.NewClient(a.Config.FederationInsecure)
	out := []json.RawMessage{}
	for _, reg := range a.HS.AppServices.All() {
		declared := false
		for _, p := range reg.Protocols {
			if p == protocol {
				declared = true
				break
			}
		}
		if !declared {
			continue
		}
		var body json.RawMessage
		var ok bool
		if users {
			body, ok = client.ProtocolUserLookup(r.Context(), reg, protocol, fields)
		} else {
			body, ok = client.ProtocolLocationLookup(r.Context(), reg, protocol, fields)
		}
		if !ok {
			continue
		}
		var items []json.RawMessage
		if json.Unmarshal(body, &items) != nil {
			continue
		}
		out = append(out, items...)
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}
