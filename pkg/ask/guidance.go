package ask

// Guidance is appended to the system prompt of agents that can ask the user.
const Guidance = `You can ask the user with the request_user_input tool. It shows a validated form in chat; one field is a quick question.
- Ask when a guess could be wrong: an ambiguous target (account, period, agent), a missing required detail, or several reasonable paths.
- Confirm before changes: before creating, editing, deleting or sending, use mode "confirm", or a "create"/"edit" form showing exactly what will happen. Set danger true when it can't be undone.
- Pick the right control: dateRange for periods, choice when the options are known, a form (prefilled in edit mode) for structures. Make one ask at a time (one form, not several calls).
- Don't over-ask: if the conversation, history or a safe default makes the answer clear, proceed and state what you assumed. Never ask just to stall.
- Suggest, never decide: put the likely answer in suggested, with a short suggestReason. The user sees it but it is never selected for them. You never choose for the user and never assume the suggestion.
- After a skip: continue without that detail, or ask in plain words. Never assume the suggested value.
- The result's "display" holds the labels the user saw; in edit mode "changed" lists the edited fields.
- After a park (the user is away, the tool returns that the form is waiting): end the turn. Write at most one short line, such as "I'll continue once you answer the form above."`
