package com.stockdice.controller;

import com.stockdice.service.DiceResult;
import com.stockdice.service.DiceService;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class HomeController {

    private final DiceService diceService;

    public HomeController(DiceService diceService) {
        this.diceService = diceService;
    }

    @GetMapping("/")
    public String index() {
        return "redirect:/en/";
    }

    @GetMapping("/en/")
    public String englishUs() {
        return "home";
    }

    @GetMapping("/en/customize/")
    public String customize() {
        return "customize";
    }

    @GetMapping("/en/roll-uniform/")
    public String rollUniform(Model model) {
        DiceResult result = diceService.rollUniform();
        if (result != null) {
            model.addAttribute("symbol", result.getSymbol());
            model.addAttribute("company_name", result.getCompanyName());
            model.addAttribute("market_cap_usd", result.getMarketCapUsd().longValue());
        } else {
            model.addAttribute("error", "No valid stocks found. The database may be empty or FMP API limits reached.");
        }
        return "roll";
    }

    @GetMapping("/en/roll-market-cap/")
    public String rollMarketCap(Model model) {
        DiceResult result = diceService.rollMarketCap();
        if (result != null) {
            model.addAttribute("symbol", result.getSymbol());
            model.addAttribute("company_name", result.getCompanyName());
            model.addAttribute("market_cap_usd", result.getMarketCapUsd().longValue());
        } else {
            model.addAttribute("error", "No valid stocks found. The database may be empty or FMP API limits reached.");
        }
        return "roll";
    }
}
