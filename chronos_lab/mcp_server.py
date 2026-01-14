from fastmcp import FastMCP
import logging

logger = logging.getLogger(__name__)

mcp = FastMCP("chronos-lab", stateless_http=True)


if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000)
