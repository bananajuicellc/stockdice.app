package com.stockdice.controller;

import com.stockdice.service.DiceResult;
import com.stockdice.service.DiceService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(HomeController.class)
public class HomeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private DiceService diceService;

    @Test
    public void testHomepageRedirect() throws Exception {
        mockMvc.perform(get("/"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("/en/"));
    }

    @Test
    public void testHomepageEn() throws Exception {
        mockMvc.perform(get("/en/"))
                .andExpect(status().isOk())
                .andExpect(view().name("home"));
    }

    @Test
    public void testRollUniform() throws Exception {
        when(diceService.rollUniform()).thenReturn(new DiceResult("AAPL", "Apple Inc.", 3000000000000.0));

        mockMvc.perform(get("/en/roll-uniform/"))
                .andExpect(status().isOk())
                .andExpect(view().name("roll"))
                .andExpect(model().attribute("symbol", "AAPL"))
                .andExpect(model().attribute("company_name", "Apple Inc."))
                .andExpect(model().attribute("market_cap_usd", 3000000000000L));
    }

    @Test
    public void testRollMarketCap() throws Exception {
        when(diceService.rollMarketCap()).thenReturn(new DiceResult("MSFT", "Microsoft Corp.", 3100000000000.0));

        mockMvc.perform(get("/en/roll-market-cap/"))
                .andExpect(status().isOk())
                .andExpect(view().name("roll"))
                .andExpect(model().attribute("symbol", "MSFT"))
                .andExpect(model().attribute("company_name", "Microsoft Corp."))
                .andExpect(model().attribute("market_cap_usd", 3100000000000L));
    }
}
