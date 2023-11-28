chrome.runtime.onMessage.addListener(listener)

async function listener(e) {
    console.log("main got message", e);

    await sleep(500)
    const cookieButton = document.querySelector(".CybotCookiebotDialogBodyButton")
    if (cookieButton != undefined) {
        console.log("found cookie button")
        cookieButton.click();
        await sleep(300)

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

    await sleep(Math.floor(Math.random() * 2200) + 800);
    const content = document.getElementsByTagName('html')[0].innerHTML
    chrome.runtime.sendMessage({
        content, url: document.URL, registerUrls: [], askNext: true
    })

}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
