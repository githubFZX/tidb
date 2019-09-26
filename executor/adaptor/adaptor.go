package adaptor

import "fmt"

//Adaptor is used to acquire strategies dynamically.
type Adaptor interface {
	InitAdaptor(name string)
	Adapt() Strategy
	GetStrategy() Strategy
}

type BaseAdaptor struct {
	pg       ParamGenerator
	sg       SceneGenerator
	mapper   *Mapper
	strategy Strategy
	rg       *Register
}

//Initialization process:
//1.Invokes the register to get the initialization method based on the name you registered
//2.Initiate the ParamGenerator and SceneGenerator
//3.New Mapper and initiate mapper
func (ba *BaseAdaptor) InitAdaptor(name string) {
	InitPGAndSG := ba.rg.registry[name]
	ba.pg, ba.sg = InitPGAndSG()
	ba.mapper = new(Mapper)
	ba.mapper.InitMapper()
}

//Startegy getting process:
//1.Invokes ParamGenerator to get statistics and hardware information.
//2.Generate scene according to data characteristics, cpu information and memory information.
//3.According to generated scene to match scene in the scene library.
//4.Use mapper to get startegy what we should use.
func (ba *BaseAdaptor) Adapt() Strategy {
	fmt.Println("begin to get strategy...")

	hwInfo := ba.pg.GetSystemState()
	statsInfo := ba.pg.GetStatistic()

	//analyze hardware information and statistics information to generate scene
	//different sg(scene generator) has different analysis method
	scene := ba.sg.GenScene(hwInfo, statsInfo)

	matchedScene, ok := ba.mapper.MatchScene(scene)
	if !ok {
		panic("All scenes are matched failed!")
	}

	strategy := ba.mapper.GetStrategy(matchedScene)
	return strategy
}

func (ba *BaseAdaptor) GetStrategy() Strategy {
	return ba.strategy
}

func (ba *BaseAdaptor) BindingToAdaptor(rg *Register) {
	ba.rg = rg
}

func (ba *BaseAdaptor) SetStrategy(sg Strategy) {
	ba.strategy = sg
}

/*func (ba *BaseAdaptor) GetPG() ParamGenerator {
	return ba.pg
}

func (ba *BaseAdaptor) GetSG() SceneGenerator {
	return ba.sg
}*/

//Register is used to provide the init method.
type Register struct {
	registry map[string]func() (ParamGenerator, SceneGenerator)
}

func NewRegister() *Register {
	rg := &Register{
		registry: make(map[string]func() (ParamGenerator, SceneGenerator)),
	}
	return rg
}

//register the method used for initiating ParamGenerator and SceneGenerator.
func (rg *Register) Register(name string, initGenerator func() (ParamGenerator, SceneGenerator)) {
	rg.registry[name] = initGenerator
}

//..................................................................................
//define our own adaptor to extend BaseAdaptor.
type HashJoinAdapter struct {
	BaseAdaptor
}
