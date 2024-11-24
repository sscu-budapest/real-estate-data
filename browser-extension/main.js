chrome.runtime.onMessage.addListener(listener)

async function listener(e) {
    console.log("main got message", e);

    await sleep(500)
    const cookieButton = document.querySelector("#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll")
    if (cookieButton != undefined) {
        console.log("found cookie button")
        cookieButton.click();
        await sleep(800)
    }

    const iframeCloudflareElem = document.querySelector("svg[id=robot-protection-illustration]")
    if (iframeCloudflareElem != undefined) {
        console.log("found cloudflare shit")
        var rect = iframeCloudflareElem.getBoundingClientRect();
        console.log(rect.top, rect.right, rect.bottom, rect.left);
        const xVal = 1426 // rect.x + 100
        const yVal = 1190 // rect.y + 140
        const runCommand = ["xdotool", "mousemove", `${xVal}`, `${yVal}`]
        chrome.runtime.sendMessage({ url: document.URL, runCommand })
        console.log("sent command", runCommand);
        console.log("sleeping waiting for click");
        await sleep(9 * 1000)
        chrome.runtime.sendMessage({ url: document.URL, runCommand: ["xdotool", "click", "1"], askNext: true })
        console.log("sleeping waiting after click");
        await sleep(20 * 1000)
    }

    if (e.navUrl) {
        const navUrl = e.navUrl;
        console.log("got navigation url", navUrl)
        window.location.assign(navUrl);
        return
    }


    const openButton = document.querySelector(".reveal-phone-number-button-container > button")
    if (openButton != undefined) {
        console.log("found phone-reveal button")
        await sleep(200)
        openButton.click();
        await sleep(300)
    }

    const authRequer = document.querySelector("#phone-reveal-auth-required-modal");
    if ((authRequer != undefined) && (authRequer.ariaHidden != 'true')) {
        console.log("authentication requester found", authRequer?.ariaHidden)
        console.log(authRequer, "end of the line")
        await sleep(3 * 60 * 1000)
        return
    }

    const sleepTime = Math.floor(Math.random() * 2200) + 800;
    console.log("sleeping ", sleepTime)
    await sleep(sleepTime);
    const content = document.getElementsByTagName('html')[0].innerHTML
    chrome.runtime.sendMessage({
        content, url: document.URL, registerUrls: [], askNext: true
    })

}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
