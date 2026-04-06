from typing import Dict, Any
from ..schemas import PartialBudgetingInput, PartialBudgetingResponse

class PartialBudgeting:
    def __init__(self):
        pass
    
    def calculate_net_benefit(self, input_data: PartialBudgetingInput) -> PartialBudgetingResponse:
        """
        Calculate net benefit using Partial Budgeting formula:
        Net Benefit = (Added Returns + Reduced Costs) - (Added Costs + Reduced Returns)
        """
        added_returns = input_data.added_returns
        reduced_costs = input_data.reduced_costs
        added_costs = input_data.added_costs
        reduced_returns = input_data.reduced_returns
        
        total_benefits = added_returns + reduced_costs
        total_costs = added_costs + reduced_returns
        
        net_benefit = total_benefits - total_costs
        
        # Determine if profitable
        is_profitable = net_benefit > 0
        
        # Generate recommendation
        if net_benefit > 0:
            recommendation = f"Proceed with the change. Expected net benefit: {net_benefit:.2f} PHP"
        elif net_benefit == 0:
            recommendation = "The change is neutral. Consider other factors before proceeding."
        else:
            recommendation = f"Do not proceed. Expected net loss: {abs(net_benefit):.2f} PHP"
        
        return PartialBudgetingResponse(
            net_benefit=net_benefit,
            is_profitable=is_profitable,
            recommendation=recommendation
        )
    
    def analyze_farming_decision(self, current_state: Dict[str, float], 
                                proposed_state: Dict[str, float]) -> PartialBudgetingResponse:
        """
        Analyze farming decision by comparing current and proposed states
        """
        # Calculate changes
        added_returns = max(0, proposed_state.get("expected_yield_value", 0) - 
                           current_state.get("current_yield_value", 0))
        
        reduced_returns = max(0, current_state.get("current_yield_value", 0) - 
                            proposed_state.get("expected_yield_value", 0))
        
        reduced_costs = max(0, current_state.get("current_costs", 0) - 
                          proposed_state.get("expected_costs", 0))
        
        added_costs = max(0, proposed_state.get("expected_costs", 0) - 
                         current_state.get("current_costs", 0))
        
        # Create input for partial budgeting
        input_data = PartialBudgetingInput(
            added_returns=added_returns,
            reduced_costs=reduced_costs,
            added_costs=added_costs,
            reduced_returns=reduced_returns
        )
        
        return self.calculate_net_benefit(input_data)
    
    def optimize_resource_allocation(self, resources: Dict[str, float], 
                                   constraints: Dict[str, float]) -> Dict[str, float]:
        """
        Optimize resource allocation using partial budgeting principles
        """
        optimized_allocation = {}
        
        # Simple optimization: allocate resources based on cost-benefit ratio
        # This is a simplified version - real implementation would be more complex
        
        total_budget = constraints.get("budget", 0)
        
        # Calculate cost-benefit ratios for each resource
        ratios = {}
        for resource, cost in resources.items():
            benefit = constraints.get(f"{resource}_benefit", cost * 1.5)  # Default benefit
            ratios[resource] = benefit / cost if cost > 0 else 0
        
        # Sort by ratio descending
        sorted_resources = sorted(ratios.items(), key=lambda x: x[1], reverse=True)
        
        # Allocate budget
        remaining_budget = total_budget
        for resource, ratio in sorted_resources:
            resource_cost = resources[resource]
            
            if resource_cost <= remaining_budget:
                optimized_allocation[resource] = resource_cost
                remaining_budget -= resource_cost
            else:
                optimized_allocation[resource] = remaining_budget
                remaining_budget = 0
            
            if remaining_budget <= 0:
                break
        
        return optimized_allocation