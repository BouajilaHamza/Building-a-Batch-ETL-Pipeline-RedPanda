from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()
app.mount("/static", StaticFiles(directory="src/static"), name="static")
templates = Jinja2Templates(directory="src/templates")


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/motherduck")
async def redirect_motherduck():
    return RedirectResponse(url="https://app.motherduck.com/")


@app.get("/solarwinds")
async def redirect_solarwinds():
    return RedirectResponse(url="https://my.solarwinds.cloud")
