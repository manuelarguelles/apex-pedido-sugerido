"""
deploy_agent.py — Crea/actualiza el agente Maxi en Azure AI Foundry.
"""
import os, json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")
from azure.ai.agents import AgentsClient
from azure.ai.agents.models import FunctionTool, FunctionDefinition
from azure.identity import AzureCliCredential
from agent.tools import TOOL_MAP, TOOL_DEFINITIONS

ENDPOINT = os.environ["AZURE_AI_FOUNDRY_PROJECT_ENDPOINT"]
INST_PATH = Path(__file__).parent / "instructions.md"

def deploy():
    cred = AzureCliCredential()
    with AgentsClient(endpoint=ENDPOINT, credential=cred) as client:
        instructions = INST_PATH.read_text()

        # Pasar tools como lista de dicts directamente (formato OpenAI)
        tools = TOOL_DEFINITIONS

        existing = None
        for a in client.list_agents():
            if a.name == "maxi-pedido-sugerido":
                existing = a; break

        if existing:
            agent = client.update_agent(
                agent_id=existing.id,
                model="gpt-4o",
                name="maxi-pedido-sugerido",
                instructions=instructions,
                tools=tools,
            )
            print(f"✅ Agente actualizado: {agent.id}")
        else:
            agent = client.create_agent(
                model="gpt-4o",
                name="maxi-pedido-sugerido",
                instructions=instructions,
                tools=tools,
            )
            print(f"✅ Agente creado: {agent.id}")

        return agent.id

if __name__ == "__main__":
    deploy()
