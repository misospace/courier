package harness

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	courierv1alpha1 "github.com/misospace/courier/api/v1alpha1"
)

// The model-surface provenance digest (#730): a canonical, versioned identity
// of the model-visible contract one native control incarnation assembles. It
// answers a postmortem question precisely — what exact model-facing contract
// did this run receive — by digesting only trusted/operator-authored
// identities, derived here from production assembly code (the pinned framing
// constants, the production tool builders) and from the run's admitted
// operator-authored envelopes (role bindings, transport, context policy, the
// pinned skill and acceptance-criterion sets). It is deliberately not a
// second description of the model surface: every section and tool identity is
// read from the same production symbols the coordinator Run path pins and
// declares, so the digest and the assembly cannot drift apart silently.
//
// Deliberately excluded, and never hashed: secrets and credentials (they are
// never inputs), volatile telemetry (request counts, resolved token budgets,
// probe results), model output and any model-authored text, workspace
// contents (including repository-local skill instruction bodies and their
// workspace-derived descriptions — the pinned bundle pairs are the trusted
// skill identity), the source-owned goal text, and any text already covered
// by an existing trusted digest or pin (criterion texts ride their pinned set
// digest; skill bodies ride the bundle digests).
//
// What the digest proves and what it does not is documented in HARNESS.md §5.

// modelSurfaceSubagentRole names the delegated sub-agent role in the
// canonical encoding. The sub-agent surface is role-generic — every delegated
// role gets the same framing and toolset — so one label covers them all.
const modelSurfaceSubagentRole = "subagent"

// modelSurfaceTaskTurnRole is the transcript role the pinned task message
// carries in production session assembly (#762 regression): the coordinator's
// goal and a delegate's brief pin as the user turn, after the system framing.
const modelSurfaceTaskTurnRole = "user"

// modelSurface is the canonical versioned preimage. Every text-bearing input
// contributes as name plus sha256 identity — never as text — so the encoding
// is bounded and safe to emit in telemetry. Field order is the wire contract;
// Go's encoding/json marshals struct fields in declaration order and sorts
// map keys, and every list below is sorted by its own key, so the encoding is
// byte-deterministic for equivalent contracts regardless of map or order
// nondeterminism at the sources.
type modelSurface struct {
	SchemaVersion    int                        `json:"schemaVersion"`
	GatewayTransport string                     `json:"gatewayTransport"`
	Roles            []modelSurfaceRole         `json:"roles"`
	Sections         []modelSurfaceSection      `json:"sections"`
	Tools            []modelSurfaceTool         `json:"tools"`
	Skills           []courierv1alpha1.SkillPin `json:"skills"`
	// Notices is the fixed-notice table: the exact text (or fmt template) of
	// every refusal, denial, argument rejection, compaction-header line, and
	// skill-result scaffolding line trusted control's own tool paths can put
	// in front of the model (#730 review). Names and digests only; the
	// dynamic parts of a templated notice are data the encoding already
	// carries elsewhere.
	Notices []modelSurfaceNotice `json:"notices"`
	// TaskTurn is the transcript role the pinned task message carries — the
	// goal on the coordinator session, the brief on a delegate session. The
	// task text itself is source-owned and never hashed, but its placement
	// is part of the model-visible contract (#762 regression): a session
	// that pinned the task as system role instead would hand the model the
	// same words through a different channel, and the digest must move.
	TaskTurn string `json:"taskTurn"`
	// ContextPolicy is the canonical JSON of the lane's declared context
	// policy as admitted; omitted when the lane declares none. Declarations
	// only: the gateway-verified effective budgets are resolved values and
	// stay out.
	ContextPolicy string `json:"contextPolicy,omitempty"`
	// AcceptanceCriteria is the pinned criterion-set digest from the
	// persisted publication policy envelope; omitted when no set was
	// pinned. The criterion texts are source text already covered by that
	// digest, so they are never re-hashed here.
	AcceptanceCriteria string `json:"acceptanceCriteria,omitempty"`
}

// modelSurfaceRole is one selected role→model binding.
type modelSurfaceRole struct {
	Role  string `json:"role"`
	Model string `json:"model"`
}

// modelSurfaceSection is one canonical pinned prompt section's identity: the
// transcript role the section pins under and the digest over its exact text.
// The role is part of the identity (#762 regression): moving a section
// between system and user placement changes what the model is told through
// which channel, so it must change the digest even when the text is
// identical.
type modelSurfaceSection struct {
	Name   string `json:"name"`
	Role   string `json:"role"`
	Digest string `json:"digest"`
}

// modelSurfaceTool is one declared tool schema's identity: the role it is
// declared to, the model's name for it, and the digest over its exact wire
// declaration (name, description, canonical parameter schema).
type modelSurfaceTool struct {
	Role   string `json:"role"`
	Name   string `json:"name"`
	Digest string `json:"digest"`
}

// modelSurfaceNotice is one fixed notice's identity: the stable name of the
// notice and the digest over its exact fixed text or fmt template — the same
// constants the emitting tool paths use verbatim.
type modelSurfaceNotice struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
}

// ModelSurfaceInputs carries the run's admitted identities the assembled
// coordinator surface does not already carry. Every value is
// trusted/operator-authored: the coordinator reads the envelopes but can
// never author them.
type ModelSurfaceInputs struct {
	// GatewayTransport is the run's declared model wire format. It is
	// resolved through the same ParseWire the transport construction uses,
	// so the digest records the effective surface, and an unknown declared
	// transport fails closed exactly as production does.
	GatewayTransport string
	// LaneFraming is the lane's additional pinned framing text, when the
	// lane declares one; it is pinned into the coordinator session as system
	// framing, after the harness framing and before the user task turn.
	LaneFraming string
	// ContextPolicy is the lane's declared context policy as admitted; nil
	// when the lane declares none.
	ContextPolicy *courierv1alpha1.ContextPolicy
	// SkillsJSON is the run's persisted operator-authored skill-pin
	// envelope (COURIER_SKILLS_JSON): name and digest pairs resolved at
	// admission. Empty when the run pins no skills.
	SkillsJSON string
	// CriteriaJSON is the run's persisted operator-authored
	// acceptance-criterion envelope (COURIER_CRITERIA), in the broker
	// criterion-set shape. Empty when no set was pinned.
	CriteriaJSON string
}

// ModelSurface is the computed provenance: the digest record that persists on
// the CoderRun plus the canonical encoding it was computed over, for
// telemetry. The encoding carries names and digests only.
type ModelSurface struct {
	// SchemaVersion is the canonical encoding version that produced Digest.
	SchemaVersion int
	// Digest is `sha256:` plus the hex sha256 of the canonical encoding.
	Digest string
	// Encoding is the canonical versioned JSON document the digest covers.
	// It carries input identities — names and digests, never prompt bodies,
	// skill bodies, or secret material — and is suitable for structured
	// run telemetry.
	Encoding []byte
}

// AssembleModelSurface computes the run's model-surface provenance (#730)
// from the production assembly the run's sessions are built from — the
// resolved role→model bindings and the production tool builders exactly as
// wired (the forge seam and the loaded skill view) — and the run's admitted
// inputs. The pre-model startup path computes and durably persists it before
// any capability probe or model execution can issue a model request (#730
// review); the coordinator's own ModelSurface delegates here with its own
// wiring, so there is one assembly source and no second surface description
// to drift. A computation failure fails closed.
func AssembleModelSurface(bindings Bindings, forge ForgeOps, skills *SkillSet, inputs ModelSurfaceInputs) (*ModelSurface, error) {
	return assembleModelSurface(bindings, coordinatorToolsFor(forge, skills), subagentToolsFor(skills), inputs)
}

// ModelSurface computes the run's model-surface provenance (#730) from this
// coordinator's own assembled surface — the pinned framing sections, the
// production tool builders exactly as wired for the run — and the run's
// admitted inputs. It delegates to AssembleModelSurface, the one assembly
// source the pre-model startup path persists from. A computation failure
// fails closed.
func (c *Coordinator) ModelSurface(inputs ModelSurfaceInputs) (*ModelSurface, error) {
	return AssembleModelSurface(c.bindings, c.forge, c.skills, inputs)
}

// assembleModelSurface is the assembly body: the canonical versioned preimage
// from the given bindings and tool builders — the exact toolsets the sessions
// declare through — plus the run's admitted inputs.
func assembleModelSurface(bindings Bindings, coordinatorTools, subagentTools []ToolDef, inputs ModelSurfaceInputs) (*ModelSurface, error) {
	transport, err := ParseWire(inputs.GatewayTransport)
	if err != nil {
		return nil, fmt.Errorf("harness: model-surface digest: %w", err)
	}
	surface := modelSurface{
		SchemaVersion:    courierv1alpha1.ModelSurfaceSchemaV2,
		GatewayTransport: transport,
		// The production role split (#762 regression): the task turn is the
		// user message; every framing section pins system.
		TaskTurn: modelSurfaceTaskTurnRole,
	}

	// Selected model bindings: the run's resolved role→model map, sorted by
	// role so map iteration order cannot reach the encoding.
	for _, role := range bindings.Roles() {
		surface.Roles = append(surface.Roles, modelSurfaceRole{Role: role, Model: bindings.Model(role)})
	}

	// Canonical pinned prompt sections, read from the production constants
	// the Run path pins, each carrying the transcript role it pins under.
	// The goal text and the skills section are deliberately absent: the goal
	// is source-owned task data, and the skills section embeds
	// workspace-derived descriptions — the pinned skill pairs below are the
	// trusted skill identity. The task turn's placement is the TaskTurn
	// field, not a section.
	sections := []modelSurfaceSection{
		{Name: "coordinator.framing", Role: "system", Digest: textDigest(harnessFraming)},
		{Name: "subagent.framing", Role: "system", Digest: textDigest(subagentFraming)},
		{Name: "control.turn", Role: "system", Digest: textDigest(controlTurnText)},
		{Name: "outcome.clarification", Role: "user", Digest: textDigest(outcomeClarification)},
	}
	// The lane's framing pins verbatim when it exists: Run pins the raw
	// text the invocation carries, so the digest hashes the exact bytes and
	// the trim decides only whether a framing exists at all — a
	// whitespace-only framing change is a prompt change and moves the
	// digest (#730 review).
	if strings.TrimSpace(inputs.LaneFraming) != "" {
		sections = append(sections, modelSurfaceSection{Name: "lane.framing", Role: "system", Digest: textDigest(inputs.LaneFraming)})
	}
	sort.Slice(sections, func(i, j int) bool { return sections[i].Name < sections[j].Name })
	surface.Sections = sections

	// Declared tool schemas per role, from the production builders exactly
	// as wired for this run: a wiring change (skills or forge) is a surface
	// change and lands here through the same code path the session declares.
	type roleTools struct {
		role  string
		tools []ToolDef
	}
	wired := []roleTools{
		{role: RoleCoordinator, tools: coordinatorTools},
		{role: modelSurfaceSubagentRole, tools: subagentTools},
	}
	for _, entry := range wired {
		tools := append([]ToolDef(nil), entry.tools...)
		sort.Slice(tools, func(i, j int) bool { return tools[i].Name < tools[j].Name })
		for _, tool := range tools {
			surface.Tools = append(surface.Tools, modelSurfaceTool{
				Role:   entry.role,
				Name:   tool.Name,
				Digest: toolDigest(tool),
			})
		}
	}

	// Fixed notices: the exact text (or fmt template) of every refusal,
	// denial, argument rejection, compaction-header line, and skill-result
	// scaffolding line trusted control's own tool paths can put in front of
	// the model (#730 review). The table holds the same constants the tool
	// paths emit verbatim, so an edit to any fixed notice changes the
	// digest; the dynamic parts of a templated notice are data the encoding
	// already carries elsewhere.
	surface.Notices = make([]modelSurfaceNotice, 0, len(fixedNoticeTable))
	for _, notice := range fixedNoticeTable {
		surface.Notices = append(surface.Notices, modelSurfaceNotice{
			Name:   notice.name,
			Digest: textDigest(notice.text),
		})
	}
	sort.Slice(surface.Notices, func(i, j int) bool { return surface.Notices[i].Name < surface.Notices[j].Name })

	// Pinned skill name+digest pairs, from the run's persisted envelope.
	if strings.TrimSpace(inputs.SkillsJSON) != "" {
		var pins []courierv1alpha1.SkillPin
		if err := json.Unmarshal([]byte(inputs.SkillsJSON), &pins); err != nil {
			return nil, fmt.Errorf("harness: model-surface digest: skill-pin envelope is malformed: %w", err)
		}
		for _, pin := range pins {
			if pin.Name == "" || pin.Digest == "" {
				return nil, fmt.Errorf("harness: model-surface digest: skill-pin envelope carries an empty name or digest")
			}
			surface.Skills = append(surface.Skills, courierv1alpha1.SkillPin{Name: pin.Name, Digest: pin.Digest})
		}
		sort.Slice(surface.Skills, func(i, j int) bool { return surface.Skills[i].Name < surface.Skills[j].Name })
	}

	// The declared context policy, canonically re-encoded. Re-marshal from
	// the decoded struct normalizes field and map-key order.
	if inputs.ContextPolicy != nil {
		encoded, err := json.Marshal(inputs.ContextPolicy)
		if err != nil {
			return nil, fmt.Errorf("harness: model-surface digest: context policy does not encode: %w", err)
		}
		surface.ContextPolicy = string(encoded)
	}

	// The pinned acceptance-criterion set identity: the digest field of the
	// persisted envelope, never the criterion texts.
	if strings.TrimSpace(inputs.CriteriaJSON) != "" {
		var envelope struct {
			Digest string `json:"digest"`
		}
		if err := json.Unmarshal([]byte(inputs.CriteriaJSON), &envelope); err != nil {
			return nil, fmt.Errorf("harness: model-surface digest: criterion envelope is malformed: %w", err)
		}
		if envelope.Digest != "" {
			surface.AcceptanceCriteria = envelope.Digest
		}
	}

	encoding, err := json.Marshal(surface)
	if err != nil {
		return nil, fmt.Errorf("harness: model-surface digest: encoding failed: %w", err)
	}
	return &ModelSurface{
		SchemaVersion: surface.SchemaVersion,
		Digest:        "sha256:" + hex.EncodeToString(textDigestBytes(encoding)),
		Encoding:      encoding,
	}, nil
}

// textDigest returns `sha256:` plus the hex sha256 of the exact text bytes.
func textDigest(text string) string {
	return "sha256:" + hex.EncodeToString(textDigestBytes([]byte(text)))
}

func textDigestBytes(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

// toolDigest digests one tool declaration's exact model-visible wire shape:
// the model's name for it, its description, and the canonical JSON of its
// parameter schema, length-prefixed with NUL separators so no boundary
// between fields is ambiguous.
func toolDigest(tool ToolDef) string {
	encoded := ""
	if len(tool.Parameters) > 0 {
		raw, err := json.Marshal(tool.Parameters)
		if err != nil {
			// Tool parameters are plain JSON-schema maps; a marshal failure
			// is a programming error, and an ambiguous identity is worse than
			// a loud one.
			panic(fmt.Sprintf("harness: tool %q parameters do not encode: %v", tool.Name, err))
		}
		encoded = string(raw)
	}
	h := sha256.New()
	for _, part := range []string{tool.Name, tool.Description, encoded} {
		fmt.Fprintf(h, "%d\x00%s", len(part), part)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}
