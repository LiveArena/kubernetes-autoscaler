# Persona
Act as a senior software architect specializing in Golang and Kubernetes.

# System Context
Cluster Autoscaler is a tool that automatically adjusts the size of the Kubernetes cluster when one of the following conditions is true:
* there are pods that failed to run in the cluster due to insufficient resources.
* there are nodes in the cluster that have been underutilized for an extended period of time and their pods can be placed on other existing nodes.

# Rules and Guidelines
- Go package in scope is `cluster-autoscaler`
- Review `cluster-autoscaler/go.mod` in the project to get deep context of the used package(s)
- Review `cluster-autoscaler/README.md` in the project to get deep context of the application(s)
- Review specific implementations related to the scope of the primary task for additional context of the required changes
- Implement new features in new files to keep file size as small as possible.
- Code duplication is acceptable to keep files as small as possible.
- Use mermaid diagrams to describe the current and changed dependencies and data flows.

# Target Audience
The plan must be clear and logical for human review and approval. It must also be highly structured and formatted for a downstream LLM to parse and execute its tasks.

# Primary Task
Create a comprehensive, linear and non-optional implementation plan to incorporate the changes found in the file `llms/knowledge/diff.txt` in the current branch.
The implementation must be added alongside current definitions. Modification of existing files must be strictly limited to calling the new alternative code paths with fallback to existing code path.
Extend existing methods in existing files with conditional logic and implement robust error handling, resource availability checks and graceful fallback.
Rather than modifying existing `type` in existing files, create new types that extend the existing existing type and store them in new files and toggle usage of the extended types.
Rather than modifying existing `const` in existing files, create new constants in new files and import them for usage in existing files to preserve existing code paths.
Rather that modifying existing `func` in existing files, create new functions in new files and import them for usage in existing files to preserve existing code paths.

# Required Sections for Correctness
The implementation plan document must be at least 500 rows to have enough details and contain the following sections to ensure a correct and verifiable outcome:

1. **Type:**: The type of work to be done, i.e. feature (FEAT), change (CH), refactor (RE), bug (BUG), maintenance (MAINT)
2. **Objectives & Rationale:** Briefly explain what we are doing and why it's beneficial and who will benefit. Describe the current state and future state.
3. **Technical Specification:** Detail the proposed changes. I.e. Specify how the configuration will be managed (e.g., via environment variables in a `.env` file).
4. **Implementation Strategy:** Detail how the proposed changes will be implemented.
5. **Implementation Plan:** Provide a step-by-step list of actions, including specific modifications details and pseudo-code snippets following insdustry best practices.
6. **Verification & Testing Plan:** Define specific tests to run. For example: "Verify the application uses the default model if none is specified" and "Verify the application can successfully use a non-default model when configured."
7. **Rollback Plan:** A clear, step-by-step procedure to safely revert the changes if verification fails.

# Output Format
Write the complete plan into a new single Markdown file in the folder "llms/plans".
The filename must start with an identifier related to the work type with a unique number followed by a descriptive short name.
Use clear Markdown headings, descriptions, code blocks for all code and configuration examples.
